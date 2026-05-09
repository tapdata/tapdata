# iEngine ↔ TM HA 部署运维要点

本文档记录 iEngine（数据同步引擎）与 TM（Manager）之间通信链路在高可用场景下的部署要求与可调参数，重点是 WebSocket 心跳/重连侧的失败转移行为。

## 1. 部署形态

支持两种 HA 形态，二选一：

### A. nginx / F5 VIP（单 URL，LB 后端多 TM 节点）

```
backend_url=http://tm-vip.tap.local/api/
```

LB 后挂多个 TM 实例，TM 节点宕机由 LB 主动剔除。引擎侧只看到一个 URL。

### B. 多 TM-URL（逗号分隔，引擎自身轮询）

```
backend_url=http://tm1:3000/api/,http://tm2:3000/api/,http://tm3:3000/api/
```

引擎在 REST 与 WebSocket 两条链路上都按列表顺序探测。WebSocket 自 v0.5.2 起会**记忆上次成功 URL**，下次重连优先尝试，避免重复支付死节点 TCP 超时。

## 2. WebSocket 心跳/重连可调参数

所有参数支持 `-D` 系统属性或同名环境变量。默认值已在 `ManagementWebsocketHandler` / `PongHandler` 中设定。

| 参数 | 默认 | 含义 |
|---|---|---|
| `WS_PONG_WAIT_MS` | `5000` | 单次 ping 等待 pong 的最长时间（ms）。超过此值视为本次 ping 失败 |
| `WS_MAX_PING_FAIL` | `3` | 连续失败次数达到此值即触发重连。语义为"≥ N 次失败" |
| `WS_HANDSHAKE_TIMEOUT_MS` | `5000` | 单个 URL 的 WebSocket handshake 上限。超时立即 cancel 并尝试下一个 URL |
| `WS_RECONNECT_TOTAL_BUDGET_MS` | `30000` | 一次 `connect()` 调用遍历所有 URL 的总预算（ms） |
| `WS_FALLBACK_DEBOUNCE_MS` | `2000` | 检测到 WS 失联后，启动 fallback scheduler（任务启停轮询）的 debounce 延迟（ms） |

**典型最坏 failover 时延（默认值下）：**
- 检测到失联：`MAX_PING_FAIL × WS_PONG_WAIT_MS + (MAX_PING_FAIL - 1) × PING_INTERVAL ≈ 3 × 5s + 2 × 10s = 35s`
- 多 URL 重连（B 场景，N URL 全断到一个健康）：`≤ N × WS_HANDSHAKE_TIMEOUT_MS = N × 5s`，并受总预算 30s 约束
- Fallback scheduler 接管：失联后 `WS_FALLBACK_DEBOUNCE_MS = 2s` 起调度

## 3. nginx / F5 timeout 配置要求（A 场景）

WebSocket 是长连接，必须保证 LB 不会比应用层心跳更快主动断开。

### 3.1 nginx

```nginx
location /ws {
    proxy_pass http://tm_upstream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";

    # 关键：必须 >= 90s，覆盖应用层心跳节奏 + 容错
    proxy_read_timeout 90s;
    proxy_send_timeout 90s;
}

upstream tm_upstream {
    least_conn;       # 推荐：在 TM 节点之间均匀分布
    server tm1:3000;
    server tm2:3000;
    server tm3:3000;
    # 默认 max_fails=1, fail_timeout=10s 已能在节点宕机时快速剔除
}
```

**不要**启用 `ip_hash` / sticky session：在引擎侧重连时会黏在已死节点上。

### 3.2 F5 BIG-IP

| 选项 | 推荐值 |
|---|---|
| Idle Timeout | >= 90 秒 |
| Profile | TCP + WebSocket profile（开启 protocol upgrade） |
| Health Monitor | TCP + HTTP `GET /api/health`，interval 5s, timeout 16s |
| Persistence | 关闭（同 nginx 理由） |

## 4. B 场景注意事项

- `backend_url` 中 URL 的顺序不影响最终一致性，但会影响**首次启动**的尝试顺序。运行期 iEngine 会记忆上次成功 URL。
- 单 URL 中**不要**包含 LB VIP 与具体节点混用——会破坏自动轮询的等价性假设。
- 与 REST 复用 `RestTemplateOperator.changeBaseURLToNext` 的轮询机制；REST/WS 失败转移**互不依赖**，可独立观测。

## 5. 排障速查

| 现象 | 可能原因 | 处置 |
|---|---|---|
| iEngine 日志反复出现 `No response was received for 3 consecutive websocket heartbeats` | TM 实际已死 / 网络高丢包 | 确认 TM 进程 + 端口；查看 LB 健康检查；必要时调大 `WS_PONG_WAIT_MS` |
| 日志出现 `Connect to web socket {url} timed out after 5000ms, will try next URL` | 该 URL 的 TCP SYN 不通（半连接 / 防火墙） | 这是正常 failover，引擎已转向下一个；如频繁出现需排查节点 |
| 重连成功但任务长时间不接管 | Fallback scheduler 未启动或被 debounce 抑制 | 检查 `WS_FALLBACK_DEBOUNCE_MS` 是否被设置过大；查看 `TapdataTaskScheduler` 日志 |
| nginx access log 频繁出现 499 / 502 但 TM 进程正常 | nginx idle_timeout 短于心跳节奏 | 调大 `proxy_read_timeout` >= 90s |
| `Reconnect total budget 30000ms exhausted` | 多个 URL 同时 silent dead | 增大 `WS_RECONNECT_TOTAL_BUDGET_MS`，或缩小 URL 列表至确为 hot 节点的子集 |

## 6. 参考代码位置

- `iengine/iengine-app/src/main/java/io/tapdata/websocket/ManagementWebsocketHandler.java`
- `iengine/iengine-app/src/main/java/io/tapdata/websocket/handler/PongHandler.java`
- `iengine/iengine-common/src/main/java/com/tapdata/mongo/RestTemplateOperator.java`（REST 侧 URL 轮询）
