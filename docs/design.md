# 可溢写分组聚合执行器 — 设计说明

## 1. 目标与边界

输入带类型的 JSONL 行和一份聚合声明，产出多层分组（`grouping_sets`）的
分组小计、渠道小计（不同分组字段子集）和总计。硬约束：

- **不得**使用 SQL `GROUP BY`、数据框（pandas 等）或任何现成聚合引擎；
  SQLite 即使引入也只能做暂存/排序，本实现干脆完全不依赖数据库。
- 核心分组、去重、聚合全部由本项目自己的代码完成。
- 全部分组状态或单个热点组的 distinct 集合都可能超过内存：实现不得把整组
  值装进 `set`，也不得在分区内先去重再直接相加。
- 输入次序和分片划分不影响逐格结果。

## 2. 总体流水线

```
typed JSONL 行
  │  逐行解析、固定类型校验（text/bool/int，bool 不是 int）
  ▼
记录发射（engine._emit_records）
  │  每行 × 每个 grouping_set × 每个聚合 → 一条定长类型化元组
  │  另有“格存在标记”，保证 FILTER 全失败时格仍由原始行定义
  ▼
外部排序（spill.ExternalSorter）
  │  内存块达阈值 → 块内排序后写 JSONL 溢写文件
  │  超 fan-in → 多级 k 路归并（每级仍是流式）
  ▼
单一全局有序流
  ▼
归并期组合（engine._combine）
  │  任意时刻只持有“当前一个格”的状态 + 每个聚合一条 distinct 上一值
  ▼
原子写出结果 JSON（os.replace 发布，失败只清理本次暂存）
```

### 为什么是“全局外排 + 单流归并”而不是分区哈希

需求明确禁止“分区内先去重再直接相加”，且要求跨溢写段的同一实际值只贡献
一次。把所有记录外排成一条全局有序流后，同一 `(格, 聚合, 值)` 的记录必然
相邻，去重退化为“与上一条比较”，于是：

- 任意 distinct 域大小都只需要 O(1) 的 distinct 驻留状态；
- 热点组再大也不会驻留集合，记录只是顺序流过；
- 两个 FILTER 不同的 distinct 聚合，其记录携带各自的聚合下标 `ai`，
  排序键不同，天然是两条独立序列，无法互相借用集合。

## 3. 记录格式与排序键

所有贡献记录统一为 5 个位置（空/非空分组集都成立）：

```
(gs_idx, *group_key, agg_idx, kind, payload)
```

- `gs_idx`：grouping_set 的声明下标；
- `group_key`：该分组集字段的值（NULL 用 `types.NULL` 哨兵）；
- `agg_idx`：聚合声明下标；标记记录用 `-1`（`MARKER_AGG`），恒排在该格
  所有贡献记录之前；
- `kind`：`0=regular`，`1=distinct`；
- `payload`：字段值；`count(*)` 用固定 int 占位 `1`，使每个元组位置
  类型同质，Python 元组比较不会出现 int 与 None 混排。

标记记录更短 `(gs_idx, *gk, -1)`，只参与前缀比较，不会越界。排序依赖
Python 元组的全序；`NULL` 哨兵定义为“小于一切非空值”，text 按 Unicode
码点、不做归一化，int 为任意精度原生整数，bool 不与 int 混排（类型由
声明固定，同一位置类型一致）。

### 溢写文件编码

每行一个 JSON 数组，标量用带标签对象编码：`{"#":"n"}`=NULL 哨兵、
`{"#":"z"}`=内部 None 占位、`{"#":"s"}`=text、`{"#":"b"}`=bool、
`{"#":"i"}`=int。Python 的 json 对整数没有 64 位限制，超大整数精确往返。

## 4. 聚合与三值逻辑 FILTER

支持：`count(*)`、`count(value)`、`sum(int)`、`min/max(int|text)`、
`count_distinct`、`sum_distinct`。

- 每个聚合可独立携带由 `eq/lt/is_null/AND/OR/NOT` 组成的 FILTER。
- 普通比较走 Kleene 三值逻辑：任一操作数为 NULL → UNKNOWN（Python
  `None`），只有 TRUE 贡献；`is_null` 恒为确定布尔；NOT(UNKNOWN)=UNKNOWN。
- distinct 忽略 NULL；同一实际值在对应分组集合和过滤范围内只贡献一次。
- `sum`/`sum_distinct` 没有非 NULL 贡献时结果为 **null 而非 0**；
  count 家族无贡献时为 0。
- 空输入：仅当声明含空分组集合 `[]` 时产出一格，count 为 0，其余为 null。
- 非空输入的格由原始行（标记记录）定义，FILTER 全失败也保留格。
- 重复同值行：普通聚合保留重数（每条 regular 记录都计入），distinct 才去重。

## 5. 内存与溢写语义

`ExternalSorter(memory_entries=N, fan_in=K)`：

- 缓冲记录数达到 N 即块内排序并落盘，因此驻留条目上限是 N（按条数，不是
  声称的字节数；实际峰值在统计 `peak_resident_entries` 中公布）。
- 溢写文件数超过 K 时做多级归并，每级把至多 K 个 run 流式合并成新 run；
  任意时刻打开的文件句柄/迭代器不超过 K。
- 最终归并也是生成器，消费结束（或被显式 `close()`）后在 finally 中关闭
  句柄并删除最终 run 文件；`run()` 的外层 finally 再对暂存目录做
  `shutil.rmtree`，成功和失败都不留本次暂存。

公布的统计（写入结果文件 `stats.sort`）：

| 指标 | 含义 |
| --- | --- |
| `records_emitted` | 引擎发射的中间记录总数 |
| `intermediate_records_written` | 各溢写轮次/归并轮次实际写盘的记录数（多级归并时 ≥ 发射数） |
| `spill_rounds` | 内存块刷盘轮数 |
| `merge_passes` | 归并轮数（含最终消费轮） |
| `spill_files_created` / `spill_bytes_written` / `spill_bytes_read` | 落盘索引与 IO 量 |
| `peak_resident_entries` | 排序器实际最大驻留条目，恒 ≤ `memory_entries_limit` |
| `fan_in` / `memory_entries_limit` | 本次执行参数 |

## 6. 输出坐标：真实 null 与 ALL 严格区分

- 被汇总掉的字段输出标记对象 `{"all": true}`；
- 真实 null 输出 JSON `null`；
- 合法文本 `"ALL"` 只是普通字符串，三者互不相同；
- 每格带 `grouping_id` 位向量：字段 `i` 不在该 grouping_set 中时置位
  `1<<i`（与 SQL `GROUPING()` 惯例同向），并带 `grouping_set` 声明下标。
- 大整数直接以 JSON number 写出（Python 端无损；消费方需支持任意精度）。

## 7. 失败原子性

- 声明校验（`check`）在任何磁盘动作之前完成；重复 grouping_set、未知字段、
  类型不符、FILTER 类型错误等一律 `DeclarationError`。
- 末行坏类型/坏 JSON 在流式读取时抛 `RowError`；此时结果文件尚未发布。
- 结果先写到暂存目录内的临时文件，`fsync` 后用 `os.replace` 原子发布；
  任何异常路径都不会在目标位置留下可读的完整结果。
- 真实写盘失败抛 `SpillError`；另提供 `simulate_write_failure` 故障注入。
- 所有异常都经 `run()` 的 finally 触发 `TempArea.cleanup()` 递归清理。

## 8. API

```python
from spillagg import check, run, read_result

normalized = check(declaration)          # 纯函数，坏声明抛 DeclarationError
stats = run(
    input_paths=["shard1.jsonl", "shard2.jsonl"],
    declaration=decl,
    result_path="out/result.json",
    scratch_dir="out/scratch",
    memory_entries=50_000,               # 远小于输入行数的缓存
    fan_in=32,
)
result = read_result("out/result.json")  # cells + stats
```

CLI：

```bash
python3 -I run_cli.py check  decl.json
python3 -I run_cli.py run --decl decl.json --input a.jsonl b.jsonl \
    --result out.json --scratch scratch/ --memory-entries 50000 --fan-in 32
python3 -I run_cli.py read out.json
```

`-I` 隔离模式忽略 PYTHONPATH 与用户 site，且不把 cwd 加入模块路径，避免
不可信输入目录中的同名模块（如 `json.py`）被导入；入口脚本显式插入项目
目录。

## 9. 测试与正确性策略

- `tests/test_agg.py`：手写所有小例格，覆盖三行演示、跨溢写段去重、真实
  null 分组、文本 `"ALL"`、两个不同 FILTER、重复同值、负数抵消、空输入、
  错误末行、坏声明、重复 grouping_set、故障注入、多归并轮、三值逻辑、
  超 64 位整数、码点排序等。
- `tests/reference.py`：独立的内存参考实现（dict + set，算法与引擎完全
  不同），在 40 组随机输入 × 多个溢写阈值/乱序/分片下逐格对账。
- `tests/scale_run.py`：运行时生成百万行（60% 集中在单一热点组、高基数
  用户域），对比多个溢写阈值的逐格结果，并以独立累计的总量做第三方校验。
  每个阈值在全新子进程中执行，父进程通过 `wait4` 取得该子进程的真实峰值
  RSS（`ru_maxrss`），因此不同阈值得到各自独立、可比的内存数字；同时公布
  全部溢写统计。大数据不入库。

## 10. 关键设计取舍

- 选外排而非哈希分区：天然满足跨段去重与“不得分区内先去重再相加”，且
  热点组无驻留集合。
- 定长、位置类型同质的记录：让 Python 原生元组全序可直接用于块排序与
  k 路堆归并，避免自写比较器的复杂度和异构类型比较错误。
- 格存在标记与聚合记录分离：FILTER 全失败的格不会消失，也不需要额外的
  组状态字典。
- 单流、单格状态：组合阶段内存与组数、distinct 域大小解耦，只与
  fan-in 和聚合个数相关。
