// Run against mongos after the replica set/sharded cluster is up.
// The local one-command stack uses a single mongod, so sharding is intentionally
// not enabled there; this script is the production/local-cluster path.
sh.enableSharding("zim");
sh.shardCollection("zim.msg", { conversation_id: "hashed", block_idx: 1 });
sh.shardCollection("zim.msg_idem", { _id: "hashed" });
