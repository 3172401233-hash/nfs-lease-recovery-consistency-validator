"""Offline NFS lease-recovery consistency validator.

This module models, as a pure offline computation, what an NFSv4-style
server must decide when a client reconnects after a network partition and
replays lease / lock / delegation state during (or after) the grace period.

Inputs are plain protocol events: clients, lease epochs, locks,
delegations, cached inodes, replayed requests, the grace period window and
the server's commit log.  The output is a :class:`RecoveryReport` listing
per-request accept status, the caches that must be invalidated and the
byte ranges (or directory entries) that must be re-read.

Only offline protocol events are handled here; no live RPC, kernel or
network interaction takes place.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class CacheKind(Enum):
    FILE = "file"
    DIRECTORY = "directory"


class RequestOp(Enum):
    RENEW_LEASE = "renew_lease"
    RECLAIM_LOCK = "reclaim_lock"
    RECLAIM_DELEGATION = "reclaim_delegation"


class RequestStatus(Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED_STALE_EPOCH = "rejected_stale_epoch"
    REJECTED_GRACE_EXPIRED = "rejected_grace_expired"
    REJECTED_LOCK_CONFLICT = "rejected_lock_conflict"


class CommitOp(Enum):
    WRITE = "write"
    TRUNCATE = "truncate"
    CREATE = "create"
    REMOVE = "remove"
    RENAME = "rename"


DIR_MODIFYING_OPS = frozenset(
    {CommitOp.CREATE, CommitOp.REMOVE, CommitOp.RENAME}
)


@dataclass(frozen=True)
class ClientState:
    """A client and the lease epoch the server currently has on record."""

    client_id: str
    current_epoch: int


@dataclass(frozen=True)
class LockRecord:
    """A lock the client believes it held before disconnecting."""

    client_id: str
    epoch: int
    fh: str
    offset: int
    length: int
    exclusive: bool = True


@dataclass(frozen=True)
class DelegationRecord:
    """A delegation the client held before disconnecting."""

    client_id: str
    fh: str
    write: bool = False


@dataclass(frozen=True)
class CachedInode:
    """Client-side cached inode state captured before the disconnect."""

    client_id: str
    fh: str
    kind: CacheKind
    change: int
    size: int = 0


@dataclass(frozen=True)
class ReplayRequest:
    """A single replayed (reclaim/renew) request from a reconnecting client."""

    request_id: str
    client_id: str
    epoch: int
    op: RequestOp
    fh: Optional[str] = None
    offset: int = 0
    length: int = 0
    arrival: float = 0.0


@dataclass(frozen=True)
class GracePeriod:
    """The server grace window; reclaims must arrive within it."""

    start: float
    end: float

    def contains(self, moment: float) -> bool:
        return self.start <= moment <= self.end


@dataclass(frozen=True)
class CommitRecord:
    """A committed server-side change made while a client was away.

    ``fh`` is the affected file; for directory-modifying ops it is the
    parent directory.  ``target_fh`` is the destination directory of a
    rename.  ``change`` is the post-op change attribute and ``size`` the
    post-op file size.
    """

    fh: str
    op: CommitOp
    client_id: str
    change: int
    offset: int = 0
    length: int = 0
    size: Optional[int] = None
    target_fh: Optional[str] = None


@dataclass
class CacheInvalidation:
    client_id: str
    fh: str
    kind: CacheKind
    reason: str


@dataclass
class RereadRange:
    client_id: str
    fh: str
    offset: int
    length: int


@dataclass
class RecoveryReport:
    request_status: Dict[str, RequestStatus] = field(default_factory=dict)
    invalidated_caches: List[CacheInvalidation] = field(default_factory=list)
    reread_ranges: List[RereadRange] = field(default_factory=list)


def _ranges_overlap(a_off, a_len, b_off, b_len):
    return a_off < b_off + b_len and b_off < a_off + a_len


def _merge_ranges(ranges):
    # type: (List[Tuple[int, int]]) -> List[Tuple[int, int]]
    """Merge overlapping/adjacent (offset, length) ranges."""
    merged = []  # type: List[List[int]]
    for off, length in sorted(ranges):
        end = off + length
        if merged and off <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([off, end])
    return [(off, end - off) for off, end in merged]


class RecoveryValidator:
    """Validates replayed recovery requests against the commit log."""

    def __init__(self, clients, locks, delegations, cached_inodes,
                 grace_period, commit_log):
        # type: (List[ClientState], List[LockRecord], List[DelegationRecord], List[CachedInode], GracePeriod, List[CommitRecord]) -> None
        self._epochs = {c.client_id: c.current_epoch for c in clients}
        self._locks = list(locks)
        self._delegations = list(delegations)
        self._cached = list(cached_inodes)
        self._grace = grace_period
        self._commit_log = list(commit_log)

    def validate(self, replay_requests):
        # type: (List[ReplayRequest]) -> RecoveryReport
        report = RecoveryReport()
        seen_request_ids = set()
        # Locks granted to *other* clients while this client was away.
        foreign_locks = [
            lock for lock in self._locks
            if lock.epoch == self._epochs.get(lock.client_id)
        ]
        lost_lock_fhs = set()  # type: set[Tuple[str, str]]

        for req in sorted(replay_requests,
                          key=lambda r: (r.arrival, r.request_id)):
            if req.request_id in seen_request_ids:
                report.request_status[req.request_id] = RequestStatus.DUPLICATE
                continue
            seen_request_ids.add(req.request_id)

            server_epoch = self._epochs.get(req.client_id)
            if server_epoch is None or req.epoch != server_epoch:
                # Old-epoch locks/leases never auto-recover.
                report.request_status[req.request_id] = (
                    RequestStatus.REJECTED_STALE_EPOCH
                )
                continue
            if not self._grace.contains(req.arrival):
                report.request_status[req.request_id] = (
                    RequestStatus.REJECTED_GRACE_EXPIRED
                )
                continue

            if req.op is RequestOp.RECLAIM_LOCK:
                conflict = any(
                    lock.client_id != req.client_id
                    and lock.fh == req.fh
                    and _ranges_overlap(
                        lock.offset, lock.length, req.offset, req.length
                    )
                    for lock in foreign_locks
                )
                if conflict:
                    report.request_status[req.request_id] = (
                        RequestStatus.REJECTED_LOCK_CONFLICT
                    )
                    lost_lock_fhs.add((req.client_id, req.fh))
                    continue

            report.request_status[req.request_id] = RequestStatus.ACCEPTED

        self._evaluate_caches(report, lost_lock_fhs)
        return report

    # ------------------------------------------------------------------
    # Cache evaluation
    # ------------------------------------------------------------------

    def _evaluate_caches(self, report, lost_lock_fhs):
        # type: (RecoveryReport, set) -> None
        invalidated = set()  # type: set[Tuple[str, str]]
        for cached in self._cached:
            key = (cached.client_id, cached.fh)
            if key in invalidated:
                continue

            # A lock the client failed to reclaim poisons the whole cache
            # for that file: bytes may have been written by the new holder.
            if key in lost_lock_fhs:
                self._invalidate(
                    report, cached, "lock-reclaim-conflict", invalidated
                )
                self._add_file_rereads(report, cached, full=True)
                continue

            # A recalled delegation means another client touched the file
            # while we cached it.
            delegation_lost = any(
                d.client_id == cached.client_id and d.fh == cached.fh
                for d in self._delegations
            ) and bool(self._foreign_commits(cached))
            if delegation_lost:
                self._invalidate(
                    report, cached, "delegation-recalled", invalidated
                )
                self._add_file_rereads(report, cached, full=True)
                continue

            commits = self._foreign_commits(cached)
            if not commits:
                continue

            if cached.kind is CacheKind.DIRECTORY:
                # Directory caches are invalidated by name-space changes
                # (create/remove/rename); re-read the whole directory.
                if any(c.op in DIR_MODIFYING_OPS for c in commits):
                    self._invalidate(
                        report, cached, "directory-modified", invalidated
                    )
                    report.reread_ranges.append(
                        RereadRange(cached.client_id, cached.fh, 0, 0)
                    )
            else:
                data_commits = [
                    c for c in commits
                    if c.op in (CommitOp.WRITE, CommitOp.TRUNCATE)
                ]
                if data_commits:
                    self._invalidate(
                        report, cached, "file-data-changed", invalidated
                    )
                    self._add_file_rereads(
                        report, cached, full=False, commits=data_commits
                    )

        # A rename dirties both the source and the target directory cache,
        # even if only one side appears directly in the commit log.
        rename_dirs = set()
        for record in self._commit_log:
            if record.op is CommitOp.RENAME:
                rename_dirs.add(record.fh)
                if record.target_fh:
                    rename_dirs.add(record.target_fh)
        for cached in self._cached:
            key = (cached.client_id, cached.fh)
            if (
                cached.kind is CacheKind.DIRECTORY
                and cached.fh in rename_dirs
                and key not in invalidated
                and self._rename_after(cached)
            ):
                self._invalidate(
                    report, cached, "directory-renamed", invalidated
                )
                report.reread_ranges.append(
                    RereadRange(cached.client_id, cached.fh, 0, 0)
                )

    def _foreign_commits(self, cached):
        # type: (CachedInode) -> List[CommitRecord]
        """Commits by other clients newer than the cached change attr."""
        return [
            record
            for record in self._commit_log
            if record.fh == cached.fh
            and record.client_id != cached.client_id
            and record.change > cached.change
        ]

    def _rename_after(self, cached):
        # type: (CachedInode) -> bool
        return any(
            record.op is CommitOp.RENAME
            and record.client_id != cached.client_id
            and record.change > cached.change
            and (record.fh == cached.fh or record.target_fh == cached.fh)
            for record in self._commit_log
        )

    def _invalidate(self, report, cached, reason, invalidated):
        # type: (RecoveryReport, CachedInode, str, set) -> None
        invalidated.add((cached.client_id, cached.fh))
        report.invalidated_caches.append(
            CacheInvalidation(cached.client_id, cached.fh, cached.kind, reason)
        )

    def _add_file_rereads(self, report, cached, full, commits=()):
        # type: (RecoveryReport, CachedInode, bool, Tuple[CommitRecord, ...]) -> None
        if cached.kind is CacheKind.DIRECTORY:
            report.reread_ranges.append(
                RereadRange(cached.client_id, cached.fh, 0, 0)
            )
            return
        truncated = any(c.op is CommitOp.TRUNCATE for c in commits)
        if full or truncated:
            new_size = cached.size
            sizes = [c.size for c in commits if c.size is not None]
            if sizes:
                new_size = max(sizes)
            report.reread_ranges.append(
                RereadRange(cached.client_id, cached.fh, 0, new_size)
            )
            return
        ranges = [(c.offset, c.length) for c in commits if c.length > 0]
        for off, length in _merge_ranges(ranges):
            report.reread_ranges.append(
                RereadRange(cached.client_id, cached.fh, off, length)
            )


def validate_recovery(clients, locks, delegations, cached_inodes,
                      replay_requests, grace_period, commit_log):
    # type: (List[ClientState], List[LockRecord], List[DelegationRecord], List[CachedInode], List[ReplayRequest], GracePeriod, List[CommitRecord]) -> RecoveryReport
    """Validate replayed recovery requests and stale caches.

    Returns a :class:`RecoveryReport` with per-request status, the caches
    that must be invalidated and the ranges that must be re-read.
    """
    validator = RecoveryValidator(
        clients, locks, delegations, cached_inodes, grace_period, commit_log
    )
    return validator.validate(replay_requests)
