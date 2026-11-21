# Mongo 备份 / 恢复演练

## 备份

```bash
./deploy/scripts/mongo-backup.sh backup
# 输出：run/backup/zim-YYYYmmdd-HHMMSS.archive.gz
```

## 恢复

```bash
./deploy/scripts/mongo-backup.sh restore run/backup/zim-YYYYmmdd-HHMMSS.archive.gz
```

## 验证

```bash
docker exec zim-dev-mongo-1 mongosh --quiet --eval \
  'db.getSiblingDB("zim").msg.countDocuments({})'
```

恢复使用 `--drop`，只允许在演练库或确认可回滚的环境执行。

## 演练记录（2026-11-16）

```text
mongodump zim          成功
mongorestore           6739 documents restored, 0 failed
zim_restore.msg        119 blocks
drop zim_restore       ok
```

恢复时使用 `--nsFrom 'zim.*' --nsTo 'zim_restore.*'` 到独立库，未覆盖生产库。
