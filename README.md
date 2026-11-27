# 镜像层缓存保留与驱逐裁定庭

# 项目生成提示词：镜像层缓存保留与驱逐裁定庭

面向容器节点缓存与发布团队，实现一个离线可运行的云原生业务控制台。场景：共享镜像层在多工作负载与离线节点间复用时，决定哪些层必须保留、哪些可以安全驱逐并保证发布包可启动。边界：以合成内容摘要和本地文件大小模拟缓存，不拉取真实镜像仓库。

使用 Python 3.11+ 标准库、SQLite、unittest；提供 `python -m layercourt` 单命令 CLI。每个命令以 SQLite IMMEDIATE 事务写入业务状态与不可变原始事件；相同业务身份和相同内容幂等，冲突内容拒绝；回放要从原始事件重建所有业务表并逐表比较。

## 离线接货与恢复（`layercourt/intake.py`）

一次只处理一个同卷本地料包，把中转目录与既有 SQLite 裁定接通：

```python
from layercourt.store import connect
from layercourt.intake import receive_package, recover_intake

db = connect("case.sqlite")
out = receive_package(
    db=db, staging="/tmp/node/transit", residence="/tmp/node/layers",
    request_id="apply-1", node="node-a", capacity=10,
    mirror=True, takeover=True,   # 两份授权同时有效
)
# 空间不足时只消耗 takeover 授权做同卷改名；mirror 授权只保留原件。
recover_intake(db=db, request_id="apply-1")  # 进程退出后从同一申请重进
```

收货前按字节长度与内容摘要校验完整清单、架构和每个物理层；相同
摘要/架构/长度的已验证驻留层经 `layer-deduplicate` 归并复用。清单、层与
`cache-record` 驻留记录在同一个最终事务里公布，调用方看不到半套可启动库存。
逐层文件移动与保管归属写入 `custody_records` 和 `.layercourt-intake/` 保管
日志；“移动一层后退出”或“事件已提交、中转目录未清理即退出”都能在重进同一
申请时幂等收敛，业务结果始终只有一份。事件处理器本身只读写 SQLite，回放不
读取或搬动任何原料文件。



运行 `python -m layercourt demo --db demo.sqlite` 与 `python -m unittest discover -s tests -v`。
