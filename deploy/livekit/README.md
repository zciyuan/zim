# 本地 LiveKit + TURN 验证

## 启动

```bash
docker compose -f deploy/compose/docker-compose.dev.yml up -d
docker compose -f deploy/compose/docker-compose.livekit.yml up -d
```

组件：

- `zim-dev-livekit`：LiveKit 1.8.4，HTTP 7880，TCP 7881，UDP 7882
- `zim-dev-coturn`：TURN 3478/tcp+udp

开发 key：

```text
devkey / zimdevsecret012345678901234567890123456789
```

## node-msg 配置

```toml
[zim.rtc]
    provider = "livekit"
    url = "http://livekit:7880"
    apiKey = "devkey"
    apiSecret = "zimdevsecret012345678901234567890123456789"
    tokenTTL = "1h"
```

## 验证

```bash
go run ./tools/rtcprobe
```

预期：

```text
room=zim-probe sid=RM_... token_len=307
```

TURN 分配/ChannelBind：

```bash
docker exec zim-dev-coturn turnutils_uclient \
  -u zim -w zimturn -p 3478 -e 127.0.0.1 127.0.0.1
```

## 真实双向音视频

`ash
npm install --prefix run/rtcweb livekit-client@2.24.1 puppeteer-core@24
go run ./tools/rtcweb -addr 127.0.0.1:8099 &
node run/rtcweb/probe.js
`

实测：`PASS room1_remote=1 room1_subscribed=audio,audio,video room2_remote=1`。

## 限制

- 已验证服务端 API、join token 签名、房间创建、TURN 端口与分配
- 尚未用真实 WebRTC 客户端做双向音视频；需要浏览器或 LiveKit SDK 客户端
- 公网部署必须替换 key/secret，并为 TURN/TLS 配置证书
