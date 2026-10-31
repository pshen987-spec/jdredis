# 方案 6：Redis I/O 线程与 CPU 对照

- `06a_io_2c2t/`：主备各绑 2 核，`io-threads=2`，其中 1 个是辅助 I/O 线程。
- `06b_io_3c3t/`：主备各绑 3 核，`io-threads=3`，其中 2 个是辅助 I/O 线程。
- 两组均设置 `io-threads-do-reads=yes`，保持 V2 的 Key、Value、命令与阶段口径。

两组分别与 `../baseline/` 比较。6A 到 6B 同时改变核数和 I/O 线程数，差额不能单独归因于其中一项。当前代码没有增加 Redis BIO 后台释放线程。
