# 基因组数据访问治理

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8304`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8304
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `dataset`：受控数据集；`application`：访问申请；`grant`：限时数据使用凭证。
- `session`：基于有效 grant 开启的访问会话，状态机为
  `requested → active → completed`，容量不足时进入 `queued`，
  grant 撤回或到期后被对账为 `frozen`（未开始）或 `stopped`（已开始）。

## 治理流程

- **容量预约**：`dataset.data.capacity` 限制同一数据集的并发活跃会话数（默认 1）。
  会话 `start` 时在单事务内检查并占用槽位；容量不足则会话进入 `queued`，
  响应中的 `release_at` 回传预计释放时间，槽位空出后可重试 `start`。
- **会话启用**：只有 `active` 状态的 grant 才能开启会话；`record` 动作在活跃会话上
  追加取用记录，`release` 释放容量槽位。
- **撤回对账**：grant `revoke`/`expire` 与对账在同一事务提交——未开始的会话冻结，
  已开始的会话停止并保留 `access_records`，每个被对账的会话都会写入审计时间线。
- **并发与重试**：同一 grant 的并发激活由乐观锁保证只放行一个；动作接口支持
  `Idempotency-Key` 请求头，写入失败整体回滚后可用同一键从上次进度重试，
  重放已提交的动作直接返回结果、不重复执行。
- **容量权限**：`set_capacity` 仅 `admin`/`committee` 可执行，且容量必须为正整数，
  越权或非法取值都会被拒绝。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

数据目录和授权凭证是治理流程演示，不包含真实数据下载、加密或机构身份联邦。
