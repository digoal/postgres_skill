# postgres_skill
PostgreSQL SKILLs for AI Agent.  
  
1. postgres-daily-check, PostgreSQL 实例日常巡检 SKILL 
2. polardb-daily-check, PolarDB 实例日常巡检 SKILL 
3. postgresql-bi-agent, PostgreSQL 实例业务数据经营分析 SKILL 
  
[Powered by AI](https://github.com/digoal/blog/blob/master/202602/20260204_01.md) 
  
用法: 将对应 skill 目录拷贝到AI Agent指定的存放SKILL的目录中.  
  
发起巡检请求, 例如: `全面巡检 postgresql 数据库, 连接: host 127.0.0.1 port 1922 user digoal dbname postgres password 123456`  
    
postgres-daily-check 技能的 PostgreSQL 巡检结果展示举例:   
  
🔍 PostgreSQL 全面巡检报告  
  
巡检时间: 2026-02-05 14:00:00    
数据库连接: 127.0.0.1:1922/postgres    
整体状态: 🟠 WARNING  
  
---  
  
一、核心健康与可用性  
  
✅ 无效索引检查  
- 状态: 正常  
- 结果: 未发现无效索引  
  
✅ XID 回卷风险  
- 状态: 正常  
- 详情:  
  - postgres: 0.60% (12,919,115 事务)  
  - test: 0.60%  
  - template1: 0.60%  
  - gemini_test: 0.60%  
  - contrib_regression: 0.60%  
- 风险: 所有数据库远低于回卷阈值(85%)  
  
✅ MultiXactId 回卷风险  
- 状态: 正常  
- 详情: 所有数据库的 datminmxid 已冻结或无效，无回卷风险  
  
✅ 阻塞锁检查  
- 状态: 正常  
- 结果: 未发现阻塞锁  
  
✅ 死锁检测  
- 状态: 正常  
- 结果: 当前数据库无死锁记录  
  
✅ 关键配置检查  
- fsync: on ✅ (数据同步正常)  
- synchronous_commit: on ✅ (同步提交正常)  
- log_min_duration_statement: -1 (未启用慢查询日志)  
- log_lock_waits: off (未启用锁等待日志)  
- track_io_timing: off (未启用IO计时)  
  
---  
  
二、会话与连接监控  
  
✅ 连接使用情况  
- 当前连接: 1/100  
- 使用率: 1.0% (正常)  
  
✅ 长时间运行查询  
- 状态: 正常  
- 结果: 未发现超过5分钟的活跃查询  
  
✅ 空闲事务会话  
- 状态: 正常  
- 结果: 未发现超过1分钟的空闲事务  
  
✅ 锁等待检查  
- 状态: 正常  
- 结果: 无锁等待  
  
✅ 等待事件  
- 状态: 正常  
- 结果: 当前无活跃等待事件  
  
🟠 连接安全性  
- SSL 加密: 0 个连接  
- GSSAPI 加密: 0 个连接  
- 本地连接: 0 个连接  
- ⚠️ 未加密连接: 1 个 (192.168.65.1)  
- 建议: 生产环境建议启用 SSL 加密连接  
  
---  
  
三、性能与活动监控  
  
❌ 缓存命中率 (严重问题)  
- 状态: 异常  
- 命中率: 56.44%  
- 问题: 缓存命中率远低于推荐值(>95%)  
- 原因: 可能存在大量全表扫描或 shared_buffers 配置不足  
- 建议:   
  1. 检查 shared_buffers 配置(当前可能过小)  
  2. 优化查询，减少全表扫描  
  3. 考虑增加内存  
  
✅ 事务回滚率  
- 状态: 正常  
- postgres: 0.00%  
- gemini_test: 0.72%  
- 其他: 0%  
- 建议: 所有数据库回滚率均在正常范围(<5%)  
  
🟡 热点表分析  
  
| Schema | 表名 | DML操作 | 扫描次数 | 死元组 |  
|--------|------|---------|----------|--------|  
| public | pgbench_accounts | 22,902,438 | 25,804,866 | 1,490,265 ⚠️ |  
| public | pgbench_tellers | 12,903,420 | 12,902,424 | 0 |  
| public | pgbench_branches | 12,902,516 | 12,902,426 | 0 |  
| public | pgbench_history | 12,902,402 | - | 0 |  
  
🟠 IO 统计  
- 临时文件: 7 个  
- 临时文件大小: 956 MB  
- 块读取: 37,739,568,299  
- 块命中: 48,897,269,759  
- 总块数: 86,636,838,058  
- 读写时间: 0ms (未启用 track_io_timing)  
  
🟠 Background Writer 统计  
- buffers_clean: 4,544,613  
- maxwritten_clean: 13,996 ⚠️  
- buffers_alloc: 37,409,810,311  
- 问题: maxwritten_clean 值较高，表明 bgwriter 可能需要调优  
- 建议: 考虑调整 bgwriter_lru_maxpages 和 bgwriter_lru_multiplier  
  
🟡 WAL 统计  
- WAL 记录: 107,729,154  
- WAL FPI: 21,586,306  
- WAL 字节: 144 GB  
- WAL buffers full: 434,597  
- 状态: WAL 生成量正常  
  
🟠 Checkpointer 统计  
- 定时检查点: 2,215 次  
- 请求检查点: 193 次  
- 完成检查点: 374 次  
- 写入时间: 8,703,830 ms  
- 同步时间: 100,240 ms  
- 缓冲区写入: 220,732  
- 平均每次检查点写入时间: 23,272.27 ms ⚠️ (偏高)  
- 平均每次检查点同步时间: 268.02 ms  
- 状态: WARNING - 检查点写入时间较长，建议检查存储性能或调整 checkpoint 参数  
  
✅ SLRU 统计  
- Transaction: 命中率 97.94% (正常)  
- Multixact Offset: 低活动量  
- Commit Timestamp: 无活动  
- 状态: 正常  
  
---  
  
四、复制与归档  
  
✅ 复制状态  
- 状态: 单机模式  
- 结果: 无活跃备库(正常运行)  
  
✅ 复制槽  
- 状态: 正常  
- 结果: 无复制槽配置  
  
🟡 WAL 归档状态  
- 状态: 未启用  
- 归档数量: 0  
- 失败数量: 0  
- WAL 目录大小: 1,024 MB  
- 建议: 如需备份恢复能力，建议启用 archive_mode 和 archive_command  
  
---  
  
五、维护与存储  
  
🟡 数据库大小  
  
| 数据库 | 大小 |  
|--------|------|  
| postgres | 3,611 MB |  
| gemini_test | 8,094 kB |  
| contrib_regression | 7,902 kB |  
| test | 7,569 kB |  
  
🟡 最大对象  
  
表:  
- pgbench_accounts: 2,346 MB  
- pgbench_history: 640 MB  
- pgbench_tellers: 78 MB  
- pgbench_branches: 64 MB  
  
索引:  
- pgbench_accounts_pkey: 428 MB  
- pgbench_tellers_pkey: 32 MB  
- pgbench_branches_pkey: 6,576 kB  
  
❌ 表膨胀 (严重问题)  
  
| Schema | 表名 | 大小 | 膨胀率 | 浪费空间 |  
|--------|------|------|--------|----------|  
| public | pgbench_accounts | 2,345.86 MB | 80.36% | 1,977 MB ⚠️ |  
| public | pgbench_history | 642.05 MB | 46.34% | 311 MB |  
| public | pgbench_tellers | 78.45 MB | 99.97% | 82 MB ⚠️ |  
| public | pgbench_branches | 64.19 MB | 100.00% | 67 MB ⚠️ |  
  
总浪费空间: 约 2.4 GB  
  
❌ 索引膨胀 (严重问题)  
  
| Schema | 索引名 | 大小 | 膨胀率 | 浪费空间 |  
|--------|--------|------|--------|----------|  
| public | pgbench_accounts_pkey | 428.44 MB | 55.43% | 249 MB |  
| public | pgbench_tellers_pkey | 31.79 MB | 99.94% | 33 MB |  
| public | pgbench_branches_pkey | 6.42 MB | 99.97% | 6.5 MB |  
  
✅ Autovacuum 状态  
- 状态: 正常  
- 结果: 当前无活跃的 autovacuum 进程  
  
✅ 大未使用索引  
- 状态: 正常  
- 结果: 未发现大未使用索引  
  
✅ 过时统计信息  
- 状态: 正常  
- 结果: 无过时统计信息  
  
✅ 序列耗尽风险  
- 状态: 正常  
- 结果: 无序列接近耗尽  
  
✅ 冻结预测  
- 状态: 正常  
- 结果: 无表接近 XID/MXID 冻结限制  
  
---  
  
六、问题汇总与建议  
  
🔴 严重问题 (需立即处理)  
  
1. 表和索引严重膨胀  
   - pgbench_tellers 膨胀率 99.97%  
   - pgbench_branches 膨胀率 100%  
   - pgbench_accounts 膨胀率 80.36%，浪费 1.9 GB 空间  
   - 建议: 立即执行 VACUUM FULL 或 REINDEX  
      VACUUM FULL pgbench_accounts;  
   VACUUM FULL pgbench_tellers;  
   VACUUM FULL pgbench_branches;  
   VACUUM FULL pgbench_history;  
   REINDEX TABLE pgbench_accounts;  
   REINDEX TABLE pgbench_tellers;  
   REINDEX TABLE pgbench_branches;  
     
2. 缓存命中率过低  
   - 当前 56.44%，远低于推荐值 95%+  
   - 建议:   
     - 增加 shared_buffers (建议设置为物理内存的 25%)  
     - 检查并优化执行大量全表扫描的查询  
     - 考虑增加服务器内存  
  
🟠 中等问题 (建议处理)  
  
3. Background Writer 调优  
   - maxwritten_clean 值较高(13,996)  
   - 建议: 调整 bgwriter 参数  
      ALTER SYSTEM SET bgwriter_lru_maxpages = 1000;  
   ALTER SYSTEM SET bgwriter_lru_multiplier = 10.0;  
     
4. Checkpointer 写入时间过长  
   - 平均每次检查点写入时间 23,272 ms  
   - 建议:   
     - 检查存储性能  
     - 调整 checkpoint_completion_target  
     - 考虑增加 max_wal_size  
5. 未加密连接  
   - 发现 1 个未加密 TCP 连接  
   - 建议: 生产环境启用 SSL  
      ssl = on  
   ssl_cert_file = 'server.crt'  
   ssl_key_file = 'server.key'  
     
🟡 低优先级建议  
  
6. WAL 归档未启用  
   - 建议: 如需 PITR 恢复能力，启用归档  
      archive_mode = on  
   archive_command = 'cp %p /path/to/archive/%f'  
     
7. 监控配置优化  
   - log_min_duration_statement: 建议设置慢查询阈值(如 1s)  
   - track_io_timing: 建议启用以监控 I/O 性能  
  
---  
  
七、巡检结论  
  
本次巡检发现 2个严重问题 和 3个中等问题:  
1. 表/索引膨胀是最紧急的问题，pgbench_tellers 和 pgbench_branches 表几乎完全膨胀，pgbench_accounts 表浪费近 2GB 空间  
2. 缓存命中率过低表明内存配置或查询优化存在问题  
3. 其他配置和性能参数有待优化  
  
建议优先级:  
1. 🔴 立即执行 VACUUM FULL 和 REINDEX 回收空间  
2. 🔴 调整 shared_buffers 提升缓存命中率  
3. 🟠 优化 bgwriter 和 checkpointer 参数  
4. 🟡 启用 SSL 加密和 WAL 归档  
  
总体评价: 数据库存在较严重的存储膨胀和内存配置问题，需要立即进行维护优化。  
  
