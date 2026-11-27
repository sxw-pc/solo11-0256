# 镜像层缓存保留与驱逐裁定庭

# 项目生成提示词：镜像层缓存保留与驱逐裁定庭

面向容器节点缓存与发布团队，实现一个离线可运行的云原生业务控制台。场景：共享镜像层在多工作负载与离线节点间复用时，决定哪些层必须保留、哪些可以安全驱逐并保证发布包可启动。边界：以合成内容摘要和本地文件大小模拟缓存，不拉取真实镜像仓库。

使用 Python 3.11+ 标准库、SQLite、unittest；提供 `python -m layercourt` 单命令 CLI。每个命令以 SQLite IMMEDIATE 事务写入业务状态与不可变原始事件；相同业务身份和相同内容幂等，冲突内容拒绝；回放要从原始事件重建所有业务表并逐表比较。



运行 `python -m layercourt demo --db demo.sqlite` 与 `python -m unittest discover -s tests -v`。

## 节点换盘盘点签认（inventory）

节点限定单节点、A/B 两个目录、一次移动、至多六个逻辑层。`storage-generation` 声明节点存储世代；`inventory-attest` 接收绑定该世代的合成目录扫描片段、跨目录搬移回执与维护契约，逐层签认当前世代的库存：

- `frozen`（维护冻结、固定目录映像）：必须拿到两目录同切点的完整映像才能确认缺项（`surface_attested`）；缺片段或切点不一致时只能等待（`frozen_image_unavailable`），等待项记为悬案且不冒充整盘验证。冻结契约内出现搬移回执直接拒绝（`frozen_contract_violated`）。
- `live`（业务持续写入）：拼接实时片段与成对搬移回执，只有源/目标切点恢复了跨片段移动边界才签认驻留；只报离开未报到、或切点边界断裂的层保持悬案。在线盘点永远不确认缺项，结果 `complete=false`。
- 片段或回执世代与节点当前世代不一致一律拒绝；旧世代的完整盘点不能影响新世代的驻留记录。
- 同切点完整映像上的缺项，若该层在扫描切点之后又有合法 `cache-record`（以事件账本中的 `at` 切点为准），只记 `scan_superseded_by_cache_record` 悬案，不得否定后来的驻留证据。
- 签认写入 `layer_residency`（按世代的最新逐层层结论）；悬案不覆盖同世代已有结论。`offline-retain` 在节点已声明世代时改用当前世代被证实可读的层闭包，既有租约保留，`inventory-doubts` 报告不再被当前世代库存支撑的批准承诺。
