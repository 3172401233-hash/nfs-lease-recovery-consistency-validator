# nfs-lease-recovery-consistency-validator
验证 NFS 租约恢复、锁重放和缓存一致性

## 概述

模拟 NFS 客户端断线重连后的恢复流程：服务器进入 grace period，客户端
回放租约续约、锁和 delegation 的 reclaim 请求。验证器根据服务器提交记录
（commit log）判断每个请求是否被接受、哪些客户端缓存必须失效，以及需要
重新读取的字节范围或目录。

**本工具只处理离线协议事件**：所有输入都是已发生的租约、锁、delegation、
缓存 inode、回放请求和提交记录等事件数据，不进行任何实时 RPC、网络或
内核交互。

## 运行环境

- Python 3.8.2+（仅标准库，无第三方依赖）

## 使用

```python
from nfs_recovery import (
    CacheKind, CachedInode, ClientState, CommitOp, CommitRecord,
    GracePeriod, LockRecord, RequestOp, ReplayRequest, validate_recovery,
)

report = validate_recovery(
    clients=[ClientState("alice", current_epoch=2)],
    locks=[LockRecord("bob", 1, "f1", offset=0, length=100)],
    delegations=[],
    cached_inodes=[CachedInode("alice", "f1", CacheKind.FILE, change=10, size=4096)],
    replay_requests=[
        ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                      fh="f1", offset=0, length=100, arrival=150.0),
    ],
    grace_period=GracePeriod(start=100.0, end=200.0),
    commit_log=[CommitRecord("f1", CommitOp.WRITE, "bob", change=11,
                             offset=0, length=64)],
)

print(report.request_status)     # 每个回放请求的接受状态
print(report.invalidated_caches) # 必须失效的缓存及原因
print(report.reread_ranges)      # 需要重新读取的范围
```

## 一致性规则

- **租约过期**：请求 epoch 与服务器当前记录不一致时拒绝
  （`rejected_stale_epoch`），旧 epoch 的锁不会自动恢复。
- **grace period**：reclaim 必须在宽限窗口内到达，否则拒绝
  （`rejected_grace_expired`）。
- **锁竞争**：断线期间其他客户端获得的重叠锁导致 reclaim 冲突
  （`rejected_lock_conflict`），对应文件缓存整体失效并全量重读。
- **目录与文件分别处理**：目录缓存由命名空间变更（create/remove/rename）
  触发失效并整目录重读；rename 同时使源目录和目标目录失效。文件数据缓存
  只由 write/truncate 触发，按提交记录合并出需要重读的字节范围；truncate
  导致全量重读。
- **delegation 召回**：持有 delegation 期间文件被其他客户端修改，整个
  文件缓存失效。
- **重复回放**：相同 `request_id` 的请求只处理一次，后续标记为
  `duplicate`，不重复应用。

## 测试

```sh
python -m unittest
```

覆盖租约过期、锁竞争、目录 rename、重复回放、delegation 召回、
写入范围合并和 truncate 等场景。
