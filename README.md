# 镜像层缓存保留与驱逐裁定庭

# 项目生成提示词：镜像层缓存保留与驱逐裁定庭

面向容器节点缓存与发布团队，实现一个离线可运行的云原生业务控制台。场景：共享镜像层在多工作负载与离线节点间复用时，决定哪些层必须保留、哪些可以安全驱逐并保证发布包可启动。边界：以合成内容摘要和本地文件大小模拟缓存，不拉取真实镜像仓库。

使用 Python 3.11+ 标准库、SQLite、unittest；提供 `python -m layercourt` 单命令 CLI。每个命令以 SQLite IMMEDIATE 事务写入业务状态与不可变原始事件；相同业务身份和相同内容幂等，冲突内容拒绝；回放要从原始事件重建所有业务表并逐表比较。



运行 `python -m layercourt demo --db demo.sqlite` 与 `python -m unittest discover -s tests -v`。


## 离线发布审批与缓存结算

- `rollout --op node-enroll` 登记节点架构。`rollout --op wave-approve` 验证一组节点的架构和逐节点层齐备性，全部就绪才写入发布批次和节点席位。
- `chargeback --op tenant-link` 给活跃工作负载引用分配租户。`chargeback --op statement-close` 对指定节点的驻留层关账。
  - 无责任协议时沿用原有结算：同一层对当前引用它的不同租户平分字节，余数按租户名称分配。
  - 随关账事件提供 `agreement`（含每个付款方的本单承诺额度 `commitments`）时切换为整层归责：每个有活跃引用的物理层按完整字节只记账一次，且只能归给一名有权承担该层全额的付款方；同一租户的多条引用不增加承接能力，任一付款方整单净额不得超过其承诺额度。
  - `backstop-commit` 登记平台对指定层、不可转借的兜底承诺额度；`backstop-authorize` / `backstop-revoke` 记录受益租户对该层兜底的授权与撤回。平台仅在该层承诺足额且取得当前全部受益租户一致授权时才成为合格付款方，兜底额度按层隔离。
  - 关账时按当前活跃引用重新确定受益人与合格付款方，因此 `retention` 的 `reference-release` 会移除不再被引用的物理层并可能使既有兜底失效。可全额关账时返回逐层责任与付款方净额；无法全额关账时拒绝并返回争用同一承诺的最小层组证据，不留下任何部分账单。
- 两套入口沿用 `--db`、`--fixture` / `--input` 和原始事件回放。回归样例见 `tests/test_rollout_chargeback.py` 与 `tests/test_chargeback_settlement.py`。
