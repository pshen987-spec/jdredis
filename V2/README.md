# V2：Redis 压测场景与独立优化

本目录按当前的六项编号整理。**方案 1～3 合在同一套基线压测代码中**，分别定义 Value、Key 和加压方式；方案 4～6 是在该基线上分别测试的优化。请勿把方案 1～3 当作三套互相独立的客户端程序。

| 编号 | 内容 | 实现位置 |
| --- | --- | --- |
| 1 | 预热 Value 大小按均值 200 KiB、范围 10～390 KiB 的截断正态分布抽样；正式 SET 固定 200 KiB | [01_value_distribution](01_value_distribution/README.md)、`baseline/stress_test_baseline_v2.cpp` |
| 2 | GET/SET 从同一 40 万 Key 池均匀随机选取；MGET 选 100 个不同 Key | [02_random_keys](02_random_keys/README.md)、`baseline/stress_test_baseline_v2.cpp` |
| 3 | 按客户端线程数做容量阶梯 | [03_load_sweep](03_load_sweep/README.md)、`baseline/run_baseline_v2_sweep.sh` |
| 4 | Redis Server 在写入前拦截达到 512000 字节的 SET/APPEND Value | [04_server_bigkey_guard](04_server_bigkey_guard/) |
| 5 | 客户端 SDK 封装 Zstd 压缩、解压和帧校验 | [05_client_sdk_zstd](05_client_sdk_zstd/) |
| 6 | Redis I/O 线程对照：2 核 2 线程、3 核 3 线程 | [06_io_threads](06_io_threads/) |

## 共同压测口径

- 186 运行 C++ 客户端，185 运行 Redis 主库 6379、备库 6380 与采集脚本；每轮单独重建 Redis。
- 每轮预热 40 万 Key，Value 大小抽样，TTL 1 秒；预热不计入阶段 2/3 OPS。
- 正式阶段普通 SET Value 固定 200 KiB，TTL 1 秒；APPEND 每次固定追加 200 KiB。`ABNORMAL_SET_SHARE` 默认为 0，可选注入 512 KiB SET。
- 阶段 2 为 30 秒，仅 MGET 和 SET；阶段 3 为 60 秒，再加入 GET 和 APPEND。1.0 倍对应 20/20/40/5 个客户端线程，MGET 一次读取 100 个 Key。
- RTT > 5 ms 是业务统计阈值；100 ms 是网络读取保护超时。阶段结果应区分发起数、成功数、错误数和实际 OPS。

## 代码与验证边界

`baseline/` 是方案 1～3 的共同实现与方案 4～6 的对照。方案 4、5、6 的客户端源码已经对齐到“预热正态分布、正式 SET 固定 200 KiB”的口径；各方案只额外改变自己负责的功能。旧的正式 SET 正态分布基线结果不能与本目录新代码直接作 A/B 对比。

方案 3 当前通过**增加线程数**加压，默认阶梯为 0.5、0.75、1.0、1.25；它没有实现固定发出 OPS，也不能保证 8000 写＋13000 读或严格等比例增长。8000/13000 仍是待核对的业务参考流量。

这里提交的是源码和操作说明，**不包含历史压测数据、编译产物或登录密码**。脚本优先使用 SSH 密钥；若测试环境需要密码，请在 185 的 shell 中设置 `CLIENT_PASS` 和 `JUMP_PASS` 环境变量，不要把密码写回仓库。正式运行前，在 186 重新编译每个客户端并执行对应操作手册中的预检。

方案 4 的 Redis 补丁仅覆盖 `setGenericCommand` 路径与 `APPEND`，不是全部写入路径。方案 5 是供本次 C++ 压测使用的 SDK 封装，尚非生产客户端 SDK。方案 6 只配置 Redis I/O 线程与 CPU 亲和范围，没有增加 BIO 后台释放线程。
