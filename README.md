# DualRoute Gateway

DualRoute Gateway 是一个 Docker 部署的多上游 OpenAI 兼容 API 网关。它提供统一的 API 地址和管理控制台，用于管理实例、网关访问密钥、上游凭据、模型开关、Mihomo 出口、审计日志与用量统计。

当前版本：`1.2.4`

## 支持的上游

| 上游 | 客户端模型名 | 实际上游模型 |
|---|---|---|
| TokenRouter | `TokenRouter/deepseek-v4-pro` | `deepseek/deepseek-v4-pro-0813-free` |
| OpenCode | `OpenCode/big-pickle` | `big-pickle` |
| OpenCode | `OpenCode/deepseek-v4-flash` | `deepseek-v4-flash-free` |
| OpenCode | `OpenCode/hy3` | `hy3-free` |
| OpenCode | `OpenCode/laguna-s-2.1` | `laguna-s-2.1-free` |
| OpenCode | `OpenCode/mimo-v2.5` | `mimo-v2.5-free` |
| OpenCode | `OpenCode/muse-spark-1.2-contributor` | `muse-spark-1.2-contributor-free` |
| OpenCode | `OpenCode/nemotron-3-ultra` | `nemotron-3-ultra-free` |
| OpenCode | `OpenCode/nemotron-3.5-lightning` | `nemotron-3.5-lightning-free` |
| OpenCode | `OpenCode/x-preview-f` | `x-preview-f-free` |
| Cline | `cline/deepseek-v4-flash` | `deepseek/deepseek-v4-flash` |
| FreeBuff | `FreeBuff/deepseek-v4-flash` | `deepseek/deepseek-v4-flash` |
| FreeBuff | `FreeBuff/deepseek-v4-pro` | `deepseek/deepseek-v4-pro` |

FreeBuff 的其他动态模型也以 `FreeBuff/<短模型名>` 展示。例如 `mimo/mimo-v2.5` 会显示为 `FreeBuff/mimo-v2.5`。模型列表每 30 分钟从动态目录刷新一次；控制面只展示已启用上游可用的模型。

OpenCode 免费模型目录每 30 分钟从 `https://opencode.ai/zen/v1/models` 动态刷新：新增的 `-free` 模型自动进入目录，上游下架的模型自动移除；内置列表仅作为首次启动前的兜底。付费模型需要上游凭据，不会出现在目录中。所有模型都可在控制台“模型管理”页单独开关。

## 功能

- 统一提供 `/v1`、`/openai/v1`、`/anthropic/v1` 与 `/codex/v1` 的兼容入口。
- 一组网关访问密钥可调用所有已启用的上游模型。
- 控制台可创建、配置、启停、重启和删除实例。
- 上游凭据按提供方保存，密钥只在服务端持久化并在界面脱敏显示。
- 新建和编辑实例时，使用密钥卡片选择已保存凭据；FreeBuff 支持同时勾选多个账号。
- TokenRouter、OpenCode、Cline、FreeBuff 可同时加入流量池。
- 支持 HTTP、HTTPS、SOCKS5 与 Mihomo 转换出口；同一上游实例不会重复分配同一出口。
- FreeBuff 实例会标识出口国家；非美国出口仅提示更换，不会自动停用。
- 支持 OpenAI Chat Completions 和 Responses；控制台提供审计、日志、Token 统计和模型开关。

## FreeBuff 多账号与会话

在“上游密钥”中保存多个 FreeBuff token。创建或编辑 FreeBuff 实例时可同时选择多个账号。

- 每个账号独立维护串行锁、会话、agent run、广告行为节流和冷却状态。
- 同一个账号一次只执行一条完整的 session/run/chat 生命周期，避免并发占用会话。
- 优先复用同模型的有效会话；没有有效会话时才轮询其他可用账号。
- 单个账号发生 `429`、空响应或生命周期失败时只冷却该账号，后续请求会跳过它。
- 网关只删除自身创建并记录的旧会话，不探测或删除其他客户端的会话。

FreeBuff 的资格、会话额度、地区门控和可用模型均由上游决定。本项目不会承诺任何模型额度或可用性。

## 部署

前置条件：Docker Engine 或 Docker Desktop，以及 Docker Compose v2。

```bash
cp config.example.env .env
docker network create dualroute-gateway-network
docker compose up -d --build
docker compose ps
```

Windows PowerShell：

```powershell
Copy-Item config.example.env .env
docker network create dualroute-gateway-network
docker compose up -d --build
```

默认地址：

| 用途 | 地址 |
|---|---|
| 控制台 | `http://127.0.0.1:13338/` |
| API | `http://127.0.0.1:13337/v1` |

首次登录账号和密码均为 `admin`。登录后必须修改密码。默认没有实例、上游密钥或网关访问密钥，需由管理员在控制台手动添加。

若使用发布包，请将 `mihomo/config.example.yaml` 复制为 `mihomo/config.yaml` 后再启动。控制台保存 Mihomo 订阅时会生成运行配置；不要将实际订阅文件提交或分享。

## 基本使用

1. 登录控制台，在“上游密钥”中保存 TokenRouter、Cline 或 FreeBuff 密钥。
2. 在“实例与出口”中创建实例，选择上游、密钥和候选出口。
3. 在“模型管理”中确认所需模型已启用。
4. 在“访问密钥”中添加一个网关密钥。
5. 等待实例显示“在线”和“流量池”，再调用 API。

```bash
curl --no-buffer http://127.0.0.1:13337/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "FreeBuff/deepseek-v4-pro",
    "messages": [{"role": "user", "content": "hello"}],
    "stream": true
  }'
```

`GET /v1/models` 返回当前实例和模型开关允许的模型。客户端 Base URL 应指向 `/v1`，不要直接暴露或调用实例内部 `13339` 端口。

## FreeBuff Token 获取

项目提供官方 CLI 授权流程的本地辅助脚本：

- Windows：双击 `get-freebuff-token.bat`
- macOS：运行 `get-freebuff-token.command`

脚本需要 Python 3，会在浏览器中打开授权页面，并将 token 写入本机的 `tools/freebuff/freebuff_token.txt`。该文件已被忽略，不在发布包中。请将 token 添加到控制台，不要公开、上传或提交该文件。

## 配置

常用变量在 [config.example.env](./config.example.env) 中说明。

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `CONTROL_HOST_PORT` | `13338` | 控制台宿主机端口 |
| `API_HOST_PORT` | `13337` | API 宿主机端口 |
| `MAX_INSTANCES` | `16` | 实例数量上限 |
| `MIHOMO_MAX_SLOTS` | `64` | Mihomo SOCKS5 槽位上限 |
| `MAX_RETRIES` | `2` | 上游失败后的不同出口额外尝试次数 |
| `REQUEST_TIMEOUT` | `5m` | 单个上游请求超时 |
| `COOLDOWN_MAX` | `60s` | 出口或 FreeBuff 账号最低冷却依据 |
| `ISOLATE_UPSTREAM_STATE` | `true` | TokenRouter 默认移除会话状态字段 |
| `FREEBUFF_MODELS_PROXY_URL` | 空 | 可选 HTTP/HTTPS 代理，仅用于控制台刷新 FreeBuff 动态模型目录 |
| `FREEBUFF_MODELS_URLS` | 官方 GitHub Release 地址 | 逗号分隔的目录地址列表，失败时按顺序使用下一个可信 GitHub 反代 |

## 升级与排错

```bash
docker compose up -d --build --force-recreate control-plane mihomo
docker compose logs --tail=100 control-plane
docker compose logs --tail=100 mihomo
docker ps -a --filter label=dualroute.gateway.managed=true
```

控制面更新不会自动替换已有动态实例。打开实例设置并保存后，该实例会使用新镜像。

## 安全与发布包

`data/`、`.env`、Mihomo 实际订阅、FreeBuff token、Docker 容器、审计日志和本地构建产物都属于私有运行数据。发布包不包含这些内容。

控制面需要访问 Docker Socket 来创建和管理实例。只应向可信管理员开放控制台；生产环境应配置 TLS、反向代理、IP 白名单和外部限流。

## 致谢

感谢以下项目提供的参考与灵感：

- [spfnas/opencode2api-free](https://github.com/spfnas/opencode2api-free)
- [GuJi08233/opencode-free-gate](https://github.com/GuJi08233/opencode-free-gate)
- [ouqiting/ds2api](https://github.com/ouqiting/ds2api)
- [cmliu/edgetunnel](https://github.com/cmliu/edgetunnel)
- [pingmike2/freebuff2api-wokers](https://github.com/pingmike2/freebuff2api-wokers)，为 FreeBuff 生命周期、动态模型目录和兼容策略提供了重要参考。

使用参考项目或其衍生代码时，请遵守各自许可证与服务条款。
