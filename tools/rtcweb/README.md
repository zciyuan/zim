# 真实音视频双向探针

## 依赖

```bash
npm install --prefix run/rtcweb livekit-client@2.24.1 puppeteer-core@24
```

## 启动

```bash
docker compose -f deploy/compose/docker-compose.livekit.yml up -d
go run ./tools/rtcweb -addr 127.0.0.1:8099
```

另开终端运行：

```bash
node run/rtcweb/probe.js
```

预期：

```text
PASS room1_remote=1 room1_subscribed=audio,audio,video room2_remote=1
```

说明：

- 两个 `livekit-client` 实例分别以 10001/10002 加入同一房间；
- 双方均发布麦克风 + 摄像头（Chrome fake device）；
- 双方都看到对方参与者和订阅轨道；
- 结果以 `PASS`/`FAIL` 写入页面标题。

## 限制

- 使用 Chrome fake media，不是真人麦克风/摄像头；
- 本机 LiveKit 为单节点，未验证多地域；
- 未验证公网 TURN/TLS、Egress 录制。
