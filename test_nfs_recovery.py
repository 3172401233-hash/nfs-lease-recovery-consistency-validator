import unittest

from nfs_recovery import (
    CacheKind,
    CachedInode,
    ClientState,
    CommitOp,
    CommitRecord,
    DelegationRecord,
    GracePeriod,
    LockRecord,
    RequestOp,
    RequestStatus,
    ReplayRequest,
    validate_recovery,
)

GRACE = GracePeriod(start=100.0, end=200.0)


def base_clients():
    return [
        ClientState("alice", current_epoch=2),
        ClientState("bob", current_epoch=1),
    ]


class LeaseExpiryTests(unittest.TestCase):
    def test_stale_epoch_lock_is_not_auto_recovered(self):
        # Alice's lease expired; the server now tracks epoch 2, but she
        # replays a reclaim from the dead epoch 1.
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[],
            replay_requests=[
                ReplayRequest("r1", "alice", 1, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=150.0),
                ReplayRequest("r2", "alice", 2, RequestOp.RENEW_LEASE,
                              arrival=151.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        self.assertEqual(report.request_status["r1"],
                         RequestStatus.REJECTED_STALE_EPOCH)
        self.assertEqual(report.request_status["r2"], RequestStatus.ACCEPTED)
        self.assertEqual(report.invalidated_caches, [])

    def test_reclaim_after_grace_period_is_rejected(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[],
            replay_requests=[
                ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=250.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        self.assertEqual(report.request_status["r1"],
                         RequestStatus.REJECTED_GRACE_EXPIRED)


class LockContentionTests(unittest.TestCase):
    def test_conflicting_reclaim_rejected_and_cache_invalidated(self):
        # While alice was away, bob took a conflicting lock on f1.
        report = validate_recovery(
            clients=base_clients(),
            locks=[
                LockRecord("bob", 1, "f1", offset=50, length=50),
            ],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
            ],
            replay_requests=[
                ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=150.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        self.assertEqual(report.request_status["r1"],
                         RequestStatus.REJECTED_LOCK_CONFLICT)
        self.assertEqual(len(report.invalidated_caches), 1)
        inv = report.invalidated_caches[0]
        self.assertEqual((inv.client_id, inv.fh), ("alice", "f1"))
        self.assertEqual(inv.reason, "lock-reclaim-conflict")
        # Whole cached file must be re-read.
        self.assertEqual(len(report.reread_ranges), 1)
        rr = report.reread_ranges[0]
        self.assertEqual((rr.offset, rr.length), (0, 4096))

    def test_non_overlapping_lock_reclaim_accepted(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[LockRecord("bob", 1, "f1", offset=500, length=50)],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
            ],
            replay_requests=[
                ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=150.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        self.assertEqual(report.request_status["r1"], RequestStatus.ACCEPTED)
        self.assertEqual(report.invalidated_caches, [])


class DirectoryRenameTests(unittest.TestCase):
    def test_rename_invalidates_source_and_target_dir_caches(self):
        # Bob renamed a file from dirA into dirB while alice cached both.
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "dirA", CacheKind.DIRECTORY, change=7),
                CachedInode("alice", "dirB", CacheKind.DIRECTORY, change=3),
                CachedInode("alice", "dirC", CacheKind.DIRECTORY, change=5),
            ],
            replay_requests=[
                ReplayRequest("r1", "alice", 2, RequestOp.RENEW_LEASE,
                              arrival=150.0),
            ],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("dirA", CommitOp.RENAME, "bob", change=8,
                             target_fh="dirB"),
            ],
        )
        invalidated = {(i.client_id, i.fh) for i in report.invalidated_caches}
        self.assertEqual(invalidated, {("alice", "dirA"), ("alice", "dirB")})
        # Directories are re-read in full (offset 0, length 0 marker).
        rereads = {(r.client_id, r.fh) for r in report.reread_ranges}
        self.assertEqual(rereads, {("alice", "dirA"), ("alice", "dirB")})
        for rr in report.reread_ranges:
            self.assertEqual((rr.offset, rr.length), (0, 0))

    def test_file_and_directory_caches_handled_separately(self):
        # A write to f1 must not invalidate the parent dir cache, and a
        # rename in dirA must not invalidate the file cache of f1.
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
                CachedInode("alice", "dirA", CacheKind.DIRECTORY, change=7),
            ],
            replay_requests=[],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("f1", CommitOp.WRITE, "bob", change=11,
                             offset=100, length=50),
                CommitRecord("dirA", CommitOp.CREATE, "bob", change=8),
            ],
        )
        by_fh = {i.fh: i for i in report.invalidated_caches}
        self.assertEqual(by_fh["f1"].reason, "file-data-changed")
        self.assertEqual(by_fh["dirA"].reason, "directory-modified")
        file_rereads = [r for r in report.reread_ranges if r.fh == "f1"]
        self.assertEqual([(r.offset, r.length) for r in file_rereads],
                         [(100, 50)])

    def test_file_write_ranges_are_merged(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
            ],
            replay_requests=[],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("f1", CommitOp.WRITE, "bob", change=11,
                             offset=0, length=128),
                CommitRecord("f1", CommitOp.WRITE, "bob", change=12,
                             offset=64, length=128),
                CommitRecord("f1", CommitOp.WRITE, "bob", change=13,
                             offset=1024, length=64),
            ],
        )
        spans = [(r.offset, r.length) for r in report.reread_ranges]
        self.assertEqual(spans, [(0, 192), (1024, 64)])

    def test_truncate_forces_full_reread(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
            ],
            replay_requests=[],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("f1", CommitOp.TRUNCATE, "bob", change=11,
                             size=512),
            ],
        )
        spans = [(r.offset, r.length) for r in report.reread_ranges]
        self.assertEqual(spans, [(0, 512)])

    def test_delegation_recall_invalidates_whole_file_cache(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[DelegationRecord("alice", "f1", write=True)],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=2048),
            ],
            replay_requests=[],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("f1", CommitOp.WRITE, "bob", change=11,
                             offset=0, length=16),
            ],
        )
        self.assertEqual(report.invalidated_caches[0].reason,
                         "delegation-recalled")
        spans = [(r.offset, r.length) for r in report.reread_ranges]
        self.assertEqual(spans, [(0, 2048)])

    def test_own_commits_do_not_invalidate(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[
                CachedInode("alice", "f1", CacheKind.FILE, change=10,
                            size=4096),
            ],
            replay_requests=[],
            grace_period=GRACE,
            commit_log=[
                CommitRecord("f1", CommitOp.WRITE, "alice", change=11,
                             offset=0, length=64),
            ],
        )
        self.assertEqual(report.invalidated_caches, [])
        self.assertEqual(report.reread_ranges, [])


class DuplicateReplayTests(unittest.TestCase):
    def test_duplicate_request_id_is_not_reapplied(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[],
            replay_requests=[
                ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=150.0),
                ReplayRequest("r1", "alice", 2, RequestOp.RECLAIM_LOCK,
                              fh="f1", offset=0, length=100, arrival=151.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        self.assertEqual(report.request_status["r1"], RequestStatus.DUPLICATE)

    def test_duplicate_of_rejected_request_stays_duplicate(self):
        report = validate_recovery(
            clients=base_clients(),
            locks=[],
            delegations=[],
            cached_inodes=[],
            replay_requests=[
                ReplayRequest("r1", "alice", 1, RequestOp.RENEW_LEASE,
                              arrival=150.0),
                ReplayRequest("r1", "alice", 2, RequestOp.RENEW_LEASE,
                              arrival=151.0),
            ],
            grace_period=GRACE,
            commit_log=[],
        )
        # The replay carries the same request id, so it dedupes regardless
        # of the (now correct) epoch.
        self.assertEqual(report.request_status["r1"], RequestStatus.DUPLICATE)


if __name__ == "__main__":
    unittest.main()
