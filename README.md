# DualRoute Gateway

一个可自托管的双上游 API 网关。控制台统一管理 TokenRouter 与 OpenCode 实例、上游 API Key、代理出口、Mihomo 订阅与请求审计；客户端只需访问一个 OpenAI 兼容 API 地址。

当前版本：**双上游版（2026-08-18）**

> 上游可用性、额度与限流由 TokenRouter 决定。增加实例或出口不等于增加上游账户额度。

## 功能概览

- 统一提供 `/v1/*` API，兼容 `/openai/v1/*`、`/anthropic/v1/*`、`/codex/v1/*`。
- 控制台创建、设置、启动、停止、重启和删除网关实例。
- 每个实例独立选择 TokenRouter 或 OpenCode，并设置其上游 API Key、并发、队列与 HTTP/HTTPS/SOCKS5 出口。
- 一组网关访问密钥可访问全部已部署的上游；控制面按客户端请求的模型名选择对应实例。
- TokenRouter 固定使用 `deepseek/deepseek-v4-pro-0813-free`，OpenCode 固定使用 `deepseek-v4-flash-free`。
- 导入 Clash/Mihomo 订阅，将 VLESS、Trojan、Shadowsocks、VMess、Hysteria2 等节点转换为本地 SOCKS5 端口。
- 实例固定使用当前健康出口；网络故障、响应截断、出口冲突或手动换线时切换出口；TokenRouter 429 冷却当前实例 Key 并由控制面选择其他实例。
- 审计、日志和 Token 统计覆盖所有已转发接口路径（包括 `/v1/chat/completions`、`/v1/responses` 和模型查询），可按接口、实例、模型、脱敏调用密钥及流式状态筛选；支持首字耗时、Token 速度和 USD 费用展示。
- `/v1/responses` 兼容 OpenAI Responses 请求；网关会按上游需要规范化函数工具、`tool_choice`、输入内容和流式生命周期。
- 实例启动后通过 `/healthz` 预检，只有健康实例进入统一 API 流量池。

### 控制台展示

![实例与出口](./image/1.png)

![Mihomo 协议转换](./image/2.png)

![审计与日志](./image/3.png)

![API Token 统计](./image/4.png)

## 快速部署

需要 Docker Engine 或 Docker Desktop 与 Docker Compose v2。Linux 主机还需让控制面访问 `/var/run/docker.sock`。

```bash
cp config.example.env .env
docker compose up -d --build
docker compose ps
```

默认端口如下：

| 用途 | 地址 |
|---|---|
| 控制台 | `http://127.0.0.1:13338/` |
| API | `http://127.0.0.1:13337/v1` |
| 实例内部端口 | `13339`，无需对外暴露 |

首次启动没有实例和访问密钥是正常状态。打开控制台，以默认账号 `admin`、密码 `admin` 登录；首次登录必须设置新密码。然后创建实例，选择上游、填写上游 API Key，并手动添加一组网关访问密钥。实例显示“在线”和“流量池”后即可调用。

## API 调用

```bash
curl --no-buffer http://127.0.0.1:13337/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek/deepseek-v4-pro-0813-free",
    "messages": [{"role": "user", "content": "hello"}],
    "stream": true
  }'
```

同一把 `GATEWAY_KEY` 支持以下固定模型：

| 客户端 `model` | 上游 |
|---|---|
| `deepseek/deepseek-v4-pro-0813-free` | TokenRouter |
| `deepseek-v4-flash-free` | OpenCode |

`GET /v1/models` 返回当前已部署实例可用的模型。Responses API 也根据请求体中的 `model` 选择上游。

OpenCode 的 `deepseek-v4-flash-free` 模型列表条目额外声明 `contextWindow: 1000000`、`supportsReasoningEffort`、`reasoningEffort` 和 `reasoningEfforts`，兼容 DSH 等客户端的推理等级选择器；默认值仍为 `none`。

客户端 Key 只用于访问网关；每个实例保存的上游 API Key 才用于访问上游。网关固定设置上游鉴权：

```text
Authorization: Bearer <TokenRouter API Key>
```

## Mihomo 与出口

在“Mihomo 协议转换”中保存服务商提供的 Clash/Mihomo HTTP(S) 订阅，然后点击“检测健康”。新建或设置实例时，选择带绿点的本地端口，例如：

```text
socks5h://mihomo:10801
socks5h://mihomo:10802
```

`vless://`、`trojan://`、`ss://` 分享链接和 Cloudflare `IP:443` 不是实例代理地址，需先由 Mihomo 或其他客户端转换为 HTTP/SOCKS5 服务。

普通请求会保持当前出口。网络错误、5xx、响应截断和重复公网出口会在实例内切换出口；TokenRouter 429 按“实例对应 Key + 模型”冷却全部出口，由控制面后续请求选择其他 Key。流式请求在 `STREAM_FIRST_OUTPUT_TIMEOUT` 内未出现文本、推理、工具调用或 Responses 完成事件时，会冷却当前出口并切换；已向客户端输出内容后不会中途重试，以免重复或拼接回复。

## 常用配置

| 变量 | 默认值 | 作用 |
|---|---:|---|
| `INSTANCE_ADMIN_TOKEN` | 自动生成 | 可选的控制面到实例内部管理接口凭据，会保存在控制数据目录 |
| `API_HOST_PORT` | `13337` | API 宿主机端口 |
| `CONTROL_HOST_PORT` | `13338` | 控制台宿主机端口 |
| `MAX_INSTANCES` | `16` | 实例数量上限 |
| `MIHOMO_MAX_SLOTS` | `64` | Mihomo SOCKS5 槽位上限，最大 `128` |
| `DIRECT_FALLBACK` | `false` | 存在代理实例时是否让直连实例参与分流 |
| `FREE_MODELS_ONLY` | `false` | 保持 TokenRouter 模型名原样，不自动追加 `-free` |
| `DISABLE_THINKING_BY_DEFAULT` | `false` | 保持客户端推理参数原样 |
| `MIN_THINKING_MAX_TOKENS` | `0` | 不调整客户端输出预算 |
| `ISOLATE_UPSTREAM_STATE` | `true` | TokenRouter 默认移除会话状态字段，避免复用上游 Worker/KV 状态 |
| `MAX_RETRIES` | `2` | 上游 429、5xx 或首输出前断流后，最多额外尝试的不同出口数 |
| `STREAM_FIRST_OUTPUT_TIMEOUT` | `20s` | 流式请求等待首个有效事件的最长时间；`0` 关闭 |
| `STREAM_FAILURE_COOLDOWN` | `10m` | 首输出前断流或超时的出口对当前模型的最低冷却时间；`0` 关闭额外冷却 |

完整变量说明见 [config.example.env](./config.example.env)。

客户端传入的模型名由实例所属上游强制替换：TokenRouter 为 `deepseek/deepseek-v4-pro-0813-free`，OpenCode 为 `deepseek-v4-flash-free`。Responses `input` 与推理参数按上游兼容策略处理。

实例设置接口使用通用字段 `upstream_api_key`、`auth_mode`；控制面保存的上游密钥位于 `data/control/upstream-keys.json`。升级时会自动读取上一版本的提供商密钥文件并迁移到新文件，已有实例无需重新录入。

## 单域名反代

宿主机 Nginx 按路径分流：

```text
/v1/、/openai/、/anthropic/、/codex/  -> 127.0.0.1:13337
/、/api/、/static/                    -> 127.0.0.1:13338
```

客户端访问 `https://gateway.example.com/v1/chat/completions`。不要直接反代单个实例的 `13339`，否则会绕过统一鉴权、调度与审计。示例见 [host-single-domain.conf.example](./nginx/host-single-domain.conf.example)。

## 升级与排错

```bash
docker compose up -d --build --force-recreate control-plane mihomo
docker compose logs --tail=100 control-plane
docker compose logs --tail=100 mihomo
docker ps -a --filter label=dualroute.gateway.managed=true
```

控制面升级后，动态实例不会自动替换。逐个打开实例设置并保存，即可使用新网关镜像。

| 现象 | 优先检查 |
|---|---|
| 创建实例返回 `502` | Docker Socket、镜像、Mihomo 健康节点、实例日志 |
| 模型返回 `429` | 审计中的 `upstream429`、模型冷却、TokenRouter Key 与上游额度 |
| `gateway_overloaded` | 实例并发、队列容量、当前请求量 |
| `unexpected EOF` | 当前出口或节点提前断开；网关会冷却并在可安全重试时切换候选出口 |
| `/v1/responses` 返回 `405` | 该接口仅接受 `POST`；客户端 Base URL 应为 `https://域名/v1` |
| 长流式首字前返回 `502` | 当前出口首输出前断流或超时；审计会记录失败出口并自动尝试其余健康出口 |
| 长请求约 125 秒后返回 Cloudflare `524` | Cloudflare 代理读取超时先于模型完成；缩短任务、关闭推理、拆分请求，或让 API 域名绕过 Cloudflare 代理 |
| 控制台样式旧 | 重建控制面容器并清理 Nginx/CDN 静态缓存 |

DualRoute Gateway 不依赖外部更新检查；升级时替换镜像并保留 `data/control` 与 Mihomo 配置即可。

## 安全

Docker Socket 具有宿主机管理权限，只应向可信管理员开放控制台。生产环境应配置 TLS、管理员认证、IP 白名单和外部限流。不要提交或公开 `.env`、`data/`、订阅地址、TokenRouter API Key、客户端 Key 与管理员 Token。

## 致谢

感谢以下项目在接口兼容、代理出口和控制台架构调研中提供的参考：

- [spfnas/opencode2api-free](https://github.com/spfnas/opencode2api-free)
- [GuJi08233/opencode-free-gate](https://github.com/GuJi08233/opencode-free-gate)
- [ouqiting/ds2api](https://github.com/ouqiting/ds2api)
- [cmliu/edgetunnel](https://github.com/cmliu/edgetunnel)

使用这些项目或其衍生代码时，请遵守各自许可证与服务条款。
