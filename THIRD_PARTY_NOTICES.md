# Third-Party Notices

ZIM is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.

This file summarizes the third-party components used by this repository and their licenses. It is not a complete bill of materials: the authoritative dependency set is `go.mod` / `go.sum`, and every module ships its own `LICENSE` in the Go module cache or upstream repository. Full license texts are available from the upstream projects.

No third-party source code is vendored in this repository; Go modules are fetched from their upstream module paths.

## Direct Go dependencies

| Module | License |
|---|---|
| `github.com/dobyte/due/v2` and the official `due` modules used here (`component/http`, `locate/redis`, `lock/redis`, `network/quic`, `network/ws`, `registry/nacos`, `transport/grpc`) | MIT |
| `github.com/gorilla/websocket` | BSD-2-Clause |
| `github.com/jackc/pgx/v5` | MIT |
| `github.com/magefile/mage` | Apache-2.0 |
| `github.com/minio/minio-go/v7` | Apache-2.0 |
| `github.com/quic-go/quic-go` | MIT |
| `github.com/quic-go/webtransport-go` | MIT |
| `github.com/redis/go-redis/v9` | BSD-2-Clause |
| `github.com/segmentio/kafka-go` | MIT |
| `go.mongodb.org/mongo-driver` | Apache-2.0 |
| `golang.org/x/crypto` | BSD-3-Clause |
| `google.golang.org/grpc` | Apache-2.0 |
| `google.golang.org/protobuf` | BSD-3-Clause |
| `github.com/BurntSushi/toml` | MIT |
| `github.com/alicebob/miniredis/v2` | MIT |

`github.com/zciyuan/ZIM/zimfw` is part of this repository and is covered by the same Apache-2.0 license (see `zimfw/LICENSE`).

## Notable transitive Go dependencies

| Module | License |
|---|---|
| `github.com/nacos-group/nacos-sdk-go/v2` | Apache-2.0 |
| `github.com/aliyun/alibaba-cloud-sdk-go` and `github.com/alibabacloud-go/*` | Apache-2.0 |
| `github.com/bytedance/sonic` | Apache-2.0 |
| `github.com/gofiber/fiber/v3` | MIT |
| `github.com/prometheus/client_golang` | Apache-2.0 |
| `github.com/klauspost/compress` | BSD-3-Clause |
| `github.com/quic-go/qpack` | MIT |
| `github.com/pires/go-proxyproto` | Apache-2.0 |
| `github.com/google/uuid` | BSD-3-Clause |
| `github.com/fsnotify/fsnotify` | BSD-3-Clause |
| `go.uber.org/zap` | MIT |
| `github.com/valyala/fasthttp` | MIT |
| `github.com/andybalholm/brotli` | MIT |
| `github.com/golang/snappy` | BSD-3-Clause |
| `github.com/xdg-go/scram` | Apache-2.0 |
| `github.com/pkg/errors` | BSD-2-Clause |
| `github.com/davecgh/go-spew` | ISC |
| `github.com/pmezard/go-difflib` | BSD-3-Clause |
| `github.com/stretchr/testify` | MIT |
| `gopkg.in/yaml.v3` | MIT / Apache-2.0 (dual) |

## External services and images

The following components are deployed as separate containers or processes. They are **not** part of this repository or its binaries; the corresponding license applies to the image/distribution you deploy.

| Component | License | Note |
|---|---|---|
| LiveKit server | Apache-2.0 | SFU used by the RTC features |
| PostgreSQL 16 | PostgreSQL License | metadata store |
| MongoDB 7 | SSPL-1.0 | message store; server is SSPL, not OSI-approved |
| Redis 7.4 | RSALv2 / SSPL-1.0 | cache, sequence, locate, idempotency; check the exact image version |
| Apache Kafka 3.8 | Apache-2.0 | message pipeline |
| Nacos 2.5 | Apache-2.0 | service registry / config |
| SeaweedFS 3.80 | Apache-2.0 | default S3-compatible object store in the dev compose file |
| MinIO server (optional) | AGPL-3.0 | `minio-go` client is Apache-2.0; the MinIO server is AGPL-3.0 |
| GoWind Admin / Vben (separate admin repository) | MIT | not bundled in this repository |

## Notes

- This repository does not include the `zim-web` client or the `zim-admin` management project; those are maintained separately and carry their own notices.
- If you redistribute ZIM binaries or container images, keep this file together with the upstream `LICENSE` / `NOTICE` files for the components you ship.
- MongoDB (SSPL-1.0), Redis 7.4 (RSALv2 / SSPL-1.0) and MinIO server (AGPL-3.0) have non-permissive terms; they are external services here, not linked code. Evaluate their terms before redistributing a bundled deployment.
- This summary is provided for convenience, not as legal advice. When in doubt, consult the upstream license text.
