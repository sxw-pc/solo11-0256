# 镜像层缓存保留与驱逐裁定庭

# 项目生成提示词：镜像层缓存保留与驱逐裁定庭

面向容器节点缓存与发布团队，实现一个离线可运行的云原生业务控制台。场景：共享镜像层在多工作负载与离线节点间复用时，决定哪些层必须保留、哪些可以安全驱逐并保证发布包可启动。边界：以合成内容摘要和本地文件大小模拟缓存，不拉取真实镜像仓库。

使用 Python 3.11+ 标准库、SQLite、unittest；提供 `python -m layercourt` 单命令 CLI。每个命令以 SQLite IMMEDIATE 事务写入业务状态与不可变原始事件；相同业务身份和相同内容幂等，冲突内容拒绝；回放要从原始事件重建所有业务表并逐表比较。



运行 `python -m layercourt demo --db demo.sqlite` 与 `python -m unittest discover -s tests -v`。

## 离线节点退役结清

`retirement` 入口处理离线节点恢复联络后上传的逐 Pod 退出/清退回执，以及整次停机的冻结快照。

- `retirement-credential` 登记逐 Pod 凭据：退出回执与清退回执必须绑定同一节点、同一工作负载 UID、同一引用代际与同一次退出；或带 `outage`/`phase` 登记停机两阶段见证。
- `retirement-outage` 冻结一次停机申请看到的引用清单及其见证；之后接纳的新代不属于该快照。
- `retirement-settle` 结清能兑现的引用。`authority=pod` 只结清被两份逐 Pod 回执覆盖的旧代；`authority=outage` 用同一停机范围两阶段一致见证结清未逐个上报的旧代。来自不同停机申请的两个半批不会拼成结清，单个请求只使用一种证据范围，并在回执中逐引用标注实际依据的 `scope` 与回执。
- 结清会把 `workload_refs` 置为 `released`；`eviction` 的现场保护裁决只读仍活跃的引用，因此已明确清退的独占层会重新进入可驱逐集合。
- 每次最多处理一台节点的两个停机申请与十条引用；既有 `workload-reference`、`reference-release` 与事件回放保持不变。
