# 方案 2：Key 全随机

实现位于 `../baseline/stress_test_baseline_v2.cpp`，与方案 1、3 共同组成 V2 基线。

- 默认 Key 池为 `test:v2:data:0`～`test:v2:data:399999`。
- SET 和单 Key GET 独立地从整个池均匀随机选 Key；没有单独的热 Key 或保活线程。
- 一次 MGET 从池里选 100 个**互不重复**的 Key；APPEND 每轮随机选一个 Key，连续追加 5 次。
- 因 SET 使用 1 秒 TTL，随机 GET/MGET 可以大量未命中。报告必须同时列请求 OPS、命中/未命中以及实际返回字节数。

这项定义了业务访问分布，不是单独的 Redis 优化。
