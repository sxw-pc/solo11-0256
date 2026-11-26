# 镜像层缓存保留与驱逐裁定庭

# 项目生成提示词：镜像层缓存保留与驱逐裁定庭

面向容器节点缓存与发布团队，实现一个离线可运行的云原生业务控制台。场景：共享镜像层在多工作负载与离线节点间复用时，决定哪些层必须保留、哪些可以安全驱逐并保证发布包可启动。边界：以合成内容摘要和本地文件大小模拟缓存，不拉取真实镜像仓库。

使用 Python 3.11+ 标准库、SQLite、unittest；提供 `python -m layercourt` 单命令 CLI。每个命令以 SQLite IMMEDIATE 事务写入业务状态与不可变原始事件；相同业务身份和相同内容幂等，冲突内容拒绝；回放要从原始事件重建所有业务表并逐表比较。



运行 `python -m layercourt demo --db demo.sqlite` 与 `python -m unittest discover -s tests -v`。


## 离线发布审批与缓存结算

- `rollout --op node-enroll` 登记节点架构。`rollout --op wave-approve` 验证一组节点的架构和逐节点层齐备性，全部就绪才写入发布批次和节点席位。
- `chargeback --op tenant-link` 给活跃工作负载引用分配租户。`chargeback --op statement-close` 对指定节点的驻留层关账。同一层对当前引用它的不同租户平分字节，余数按租户名称分配。一次关账结果随后由归档台账固定。
- 两套入口沿用 `--db`、`--fixture` / `--input` 和原始事件回放。回归样例见 `tests/test_rollout_chargeback.py`。
