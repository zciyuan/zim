# ZIM Helm 生产规格

## 应用

```bash
helm template zim deploy/helm/zim -f deploy/helm/zim/values.yaml
helm upgrade --install zim deploy/helm/zim -n zim --create-namespace \
  -f deploy/helm/zim/values.yaml
```

关键规格：

| 服务 | 副本 | requests | limits |
|---|---:|---|---|
| zim-gate | 20 | 4C/4Gi | 8C/8Gi |
| zim-transfer | 6 | 2C/2Gi | 4C/4Gi |
| zim-push | 8 | 2C/2Gi | 4C/4Gi |
| zim-node-msg | 4 | 2C/2Gi | 4C/4Gi |
| zim-api | 4 | 2C/2Gi | 4C/4Gi |
| zim-mesh | 3 | 2C/2Gi | 4C/4Gi |

HPA：

```text
gate 20→60 CPU 65%
transfer 6→24 CPU 65%
push 8→30 CPU 70%
api 4→16 CPU 70%
node-msg 4→20 CPU 70%
```

PDB：

```text
maxUnavailable: 1
```

## 中间件

```bash
# 以下为上游 chart 的覆盖 values 示例，不随 ZIM 应用 chart 安装。
helm upgrade --install kafka bitnami/kafka -f deploy/helm/infra-values.yaml
helm upgrade --install mongodb bitnami/mongodb-sharded -f deploy/helm/infra-values.yaml
helm upgrade --install redis bitnami/redis -f deploy/helm/infra-values.yaml
helm upgrade --install postgresql bitnami/postgresql -f deploy/helm/infra-values.yaml
```

规格：

```text
Kafka   3 broker, RF=3, min.insync=2
Mongo   3 shards × 3 replicas + 3 mongos + 3 config
Redis   3 primary + 3 replica + sentinel
PG      1 primary + 2 sync replica + PgBouncer ×2
```

实际安装前必须按云厂商 StorageClass 调整 `persistence.size` 和 node affinity。
