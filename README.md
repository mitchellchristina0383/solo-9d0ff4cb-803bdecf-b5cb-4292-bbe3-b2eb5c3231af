# 五端口虚拟交换机实验台

Flask 实现的 5 端口 VLAN 感知学习型交换机，供值班员在实验台切换接入网段时
核对积压帧去向。

## 功能

- 网页改配任意端口 VLAN（1–4094）
- 发送带源/目的 MAC 的单播帧（非法 MAC、非单播目的会被拒绝）
- 每端口独立「暂停出帧 / 放行出帧」：暂停期间发往该口的帧**暂存到本地磁盘**
  （端口队列只保留帧号等元数据），放行时按 FIFO 从磁盘逐个取回、全部交付
- **内容寻址去重**：同一帧内容（帧头+payload 的 SHA-256）积压在多个端口时，
  磁盘只存一份，由引用计数共享；放行/改配只释放本口引用，最后一处待发项
  移除后才删除磁盘文件
- **写盘失败原子作废**：暂存写入失败时整次发帧回滚，MAC 表、帧号、各口
  待发与实收保持原样，网页返回 400 并提示原因
- **固定内存上限**：帧内容读盘经过容量 32 的有界 LRU 缓存，发帧、放行与
  网页自动刷新处理任意长度积压时，帧内容占用的内存都有固定上限
- 端口卡片只显示积压总数与队尾 10 帧；`/api/state` 默认同样只重建队尾
  10 帧（自动刷新内存有界），完整积压明细（含 payload）用
  `/api/state?full=1` 显式查询；默认响应的 `queue` 仅含帧号/摘要元数据
- 查看各端口已收到的帧（最近 500 帧，已收总数全量计入）、当前积压、暂存概况、事件日志
- 各 VLAN 独立学习源 MAC；已知目的按表转发，未知目的只泛洪到本 VLAN 其他端口
- 带积压改配：改配生效瞬间，指向该口的学习项（跨所有 VLAN）与该口待发队列
  同时失效；切回原 VLAN 也按空队列处理；其他端口积压照常交付
- 同一端口的改配与放行可并发发起：核心模型用一把可重入锁把所有操作串行化，
  每个操作原子生效

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
# 暂存目录挂在命名卷 switch-spool（容器内 /var/lib/vlan-switch/spool）
# ENABLE_TEST_HOOKS=1 开放 /api/test/fail-writes 故障注入钩子，便于自测
```

### 本地直接运行

```bash
pip install -r requirements.txt
SPOOL_DIR=/tmp/vlan-switch-spool python app.py   # http://localhost:8000
```

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `SPOOL_DIR` | `/tmp/vlan-switch-spool` | 积压帧磁盘暂存目录，启动时自动清空 |
| `ENABLE_TEST_HOOKS` | 未设置 | 置 `1` 开放 `POST /api/test/fail-writes` 故障注入钩子 |

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页控制台 |
| GET | `/api/state` | 状态（默认有界：每口仅队尾 10 帧含内容） |
| GET | `/api/state?full=1` | 完整积压明细（含全部 payload） |
| POST | `/api/frame` | 发帧 `{port, src, dst, payload}` |
| POST | `/api/port/<id>/vlan` | 改配 VLAN `{vlan}` |
| POST | `/api/port/<id>/pause` | 暂停出帧 |
| POST | `/api/port/<id>/release` | 放行出帧（按 FIFO 交付积压） |
| POST | `/api/reset` | 复位整机（同时清空磁盘暂存） |
| POST | `/api/test/fail-writes` | 故障注入 `{enabled: bool}`（仅 `ENABLE_TEST_HOOKS=1`） |

错误输入与暂存写盘失败返回 `400 {"error": ...}`；`/api/state` 额外返回
`spool` 暂存概况（磁盘份数、引用数、读缓存条目/上限）与每端口的
`queue_tail`（队尾 10 帧，含 payload）；默认 `queue` 只含帧号与 blob
摘要元数据，`?full=1` 时才含全部帧内容。

## 设计要点

- `switch.py` 为纯模型：`Switch` 持有一把 `threading.RLock`，
  `send_frame / set_vlan / pause / release / state` 全部在锁内完成，
  因此改配与放行并发时只是被串行化，不会交错破坏状态。
- `SpoolStore` 是内容寻址暂存仓库：`put` 按整帧 SHA-256 摘要落盘
  （`tempfile + os.replace` 原子替换，不会留半截文件）并维护引用计数；
  `get` 走容量 `SPOOL_CACHE_LIMIT=32` 的有界 LRU 缓存；`release` 在
  最后一处引用消失时删除文件。所有调用都在 Switch 锁内，引用计数与
  文件操作对各临界区原子。
- `send_frame` 先做学习/转发决策，再逐口暂存或交付；任一暂停口写盘
  失败即抛出 `SpoolWriteError` 并回滚本帧已做的全部修改（撤销已暂存
  待发项、已交付计数、学习项与帧号），实现「整次发帧作废、状态原样」。
- `set_vlan` 在同一临界区内依次：改端口 VLAN → 删除所有 VLAN 表中指向
  本口的学习项 → 清空本口积压并逐项释放暂存引用。三者对观察者原子。
- 学习表按 VLAN 分表（`{vlan: {mac: port}}`），同一 MAC 在不同 VLAN 独立学习；
  同一 MAC 换端口出现则迁移表项。
- 不变式（`Switch.check_invariants`，测试与冒烟均断言）：
  学习项必指向当前处于该 VLAN 的端口；有积压的端口必处于暂停态；
  暂存引用计数等于各口待发项总数；每个被引用的 blob 都有磁盘文件；
  读缓存不超过固定上限。
- 复位与重启：`reset` 清空引用、缓存与暂存目录；`Switch` 初始化时也会
  清空暂存目录，因此服务重启从空交换机开始。

## 测试

```bash
python -m unittest discover -s tests -v   # 单元测试（模型 + API + 暂存）
python scripts/smoke_http.py              # 起真实服务跑端到端场景（含暂存）
```

## 已知限制

- 使用 Flask 内置开发服务器（threaded），未接 gunicorn 等生产级 WSGI
- 状态不持久化，进程重启即丢（暂存目录启动时清空，从空交换机开始）；
  无认证，仅限实验室使用
- 只支持单播帧（广播/组播目的被拒绝）；无 STP、无端口镜像等
- 待发项元数据队列本身随积压长度增长（每项仅帧号+摘要，约百字节）；
  帧内容（含 payload）全部在磁盘，读缓存固定 32 条，处理积压的帧内容
  内存有固定上限；每端口已收帧历史保留最近 500 条
