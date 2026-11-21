# MongoDB 分片

本地 `docker-compose.dev.yml` 使用单个 `mongod`，无法执行 `sh.shardCollection`。生产/分片验证环境需：

1. 启动 config server、至少两个 shard 与 mongos；
2. 用 `mongosh mongos:27017/zim deploy/mongo/sharding.js` 执行分片；
3. 用 `db.msg.getShardDistribution()` 与带 `conversation_id` 的 `explain()` 验证同一会话的块落在同一 shard。`docker compose -f deploy/compose/docker-compose.mongo-shard.yml up -d` 可一键起临时验证集群（端口 27023）。

`zim.msg` 复合分片键为 `{ conversation_id: "hashed", block_idx: 1 }`；应用侧 `EnsureIndexes` 已创建同名复合索引。`zim.msg_idem` 以 `_id` 命中唯一去重，单独按 `_id` hashed 分片。
