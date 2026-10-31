# 方案 3：线程阶梯加压

实现位于 `../baseline/run_baseline_v2_sweep.sh`、`collect_baseline_v2_metrics.sh` 和 `stress_test_baseline_v2.cpp`。

- 每档重新建立主备 Redis、预热同一 Key 池，再分别测阶段 2 的 30 秒及阶段 3 的 60 秒。
- 线程数按 `round(倍率 × 基准线程数)` 计算；1.0 倍为 MGET 20、SET 20、GET 40、APPEND 5。阶段 2 只启用 MGET/SET，阶段 3 四类命令均启用。
- 默认倍率为 0.5、0.75、1.0、1.25，即步长 0.25。需要 0.5 步长时可设置 `STEP_MULTIPLIER=0.5`。
- MGET、GET 收到回复后继续请求；SET 成功后等待 2 ms；APPEND 每连续 5 次后等待 100 ms。当前没有按目标 OPS 定时发送请求。

**线程倍率不等于 OPS 倍率。** 8000 写＋13000 读是参考流量，当前代码不能保证达到或按比例放大。容量报告需要列出每档实际发起数、成功 OPS、RTT>5 ms 比例和错误数。
