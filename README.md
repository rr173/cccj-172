# spillagg — 可溢写的分组聚合执行器

纯 Python 实现的外排归并分组聚合：带类型 JSONL 输入 + 聚合声明，产出
`grouping_sets` 各层小计与总计。不使用 SQL `GROUP BY`、数据框或任何现成
聚合引擎；分组、去重、聚合全部在本仓库代码内完成，且全部分组状态或单个
热点组的 distinct 集合都可以超过内存。

## 目录

```
spillagg/
  types.py       # NULL 哨兵、固定类型检查、溢写文件类型化 JSON 编解码
  filter.py      # FILTER 声明校验 + 三值逻辑（Kleene）求值
  declaration.py # 聚合声明校验（含重复 grouping_set 拒绝）、digest
  spill.py       # TempArea + ExternalSorter：受限内存外排、多级 k 路归并
  engine.py      # check / run / read_result API、记录发射、归并期组合、原子发布
  cli.py         # check / run / read 命令行
tests/
  test_agg.py    # 全语义测试（手写小例 + 随机枚举）
  reference.py   # 独立内存参考实现（不同算法，用于对账）
  scale_run.py   # 百万行/热点组/高去重域规模运行器与真实 RSS 测量
  run_tests.py   # 隔离模式测试入口
fixtures/        # 小型提交夹具（声明 + 三行演示数据）
docs/design.md   # 设计说明
```

## 快速开始

```bash
# 声明校验
python3 -I run_cli.py check fixtures/demo_decl.json

# 执行（缓存刻意调到远小于输入规模，强制溢写与归并）
python3 -I run_cli.py run \
  --decl fixtures/demo_decl.json \
  --input fixtures/demo_rows.jsonl \
  --result /tmp/demo-result.json \
  --scratch /tmp/demo-scratch \
  --memory-entries 3 --fan-in 4

python3 -I run_cli.py read /tmp/demo-result.json
```

三行演示 `(A,u,10,true) (B,u,20,false) (A,v,null,true)` 的结果：

| 层 | sum(amount) | count_distinct(user) 审核通过 | sum_distinct(amount) |
| --- | --- | --- | --- |
| region=A | 10 | 2 | 10 |
| region=B | 20 | 0（FILTER 全失败，格仍保留） | 20 |
| 总计（ALL） | 30 | 2 | 30 |

## 跑测试

```bash
python3 -I tests/run_tests.py
```

规模运行（数据运行时生成，不提交；每个阈值在独立子进程执行，
`wait4` 取得各自真实峰值 RSS）：

```bash
python3 -I tests/scale_run.py --rows 1000000 \
  --workdir data/scale --thresholds 20000 50000 150000 --fan-in 32
```

故障与坏数据夹具（均不产生可读结果，只清理本次暂存）：

```bash
python3 -I run_cli.py check fixtures/bad_duplicate_set_decl.json   # exit 2
python3 -I run_cli.py run --decl fixtures/demo_decl.json \
  --input fixtures/bad_last_row.jsonl --result /tmp/r.json \
  --scratch /tmp/sc --memory-entries 2                             # exit 3
python3 -I run_cli.py run --decl fixtures/demo_decl.json \
  --input fixtures/demo_rows.jsonl --result /tmp/r.json \
  --scratch /tmp/sc --memory-entries 2 --simulate-write-failure    # exit 4
```

## 关键语义

- 字段类型由声明固定：可空 `text` / `bool` / 任意精度 `int`，bool 不当 int。
- 聚合：`count(*)`、`count(value)`、`sum`、`min/max`、`count_distinct`、
  `sum_distinct`，每个聚合可独立带 `eq/lt/is_null/AND/OR/NOT` FILTER。
- 普通比较三值逻辑，仅 TRUE 贡献；distinct 忽略 NULL，跨溢写段同一实际值
  只贡献一次；两个不同 FILTER 的 distinct 互不借用集合。
- `sum` 无非 NULL 贡献为 null（不是 0）；空输入只有空分组集合生成一格。
- 输出坐标区分真实 null（JSON `null`）与被汇总的 ALL（`{"all": true}`），
  合法文本 `"ALL"` 只是普通字符串；每格带 `grouping_id` 与分组集下标。
- 超大整数精确求和；text 按码点比较、不归一化。
- 结果通过临时文件 + `os.replace` 原子发布；坏声明、末行坏类型、真实/模拟
  写盘失败都不会产生可读完整结果，只清理本次暂存。

详见 `docs/design.md`。
