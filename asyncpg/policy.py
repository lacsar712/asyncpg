# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


"""Runtime execution-policy stack.

This module implements a *runtime*, scoped policy layer that is fully
orthogonal to the permanent connection/pool configuration
(``command_timeout``, ``min_size``, ``max_size``,
``max_inactive_connection_lifetime``), the type codec registry, the
replication stream, and prepared-statement name management.

The stack is bound to the current *cooperative scope* (an
``async with`` / ``with`` block and everything it awaits).  It is backed
by a :class:`contextvars.ContextVar`, so two concurrent tasks never
share a frame: tasks copy the context when spawned and diverge on their
first push, while nested scopes always re-read the current top of
stack.  Nothing is ever stashed on ``Connection``, ``Pool``,
``Cursor``, ``Transaction`` or task objects.

Each frame carries three independent axes:

* ``exec_admit`` -- whether new statements / cursor result fetches may
  be admitted (``honor_cfg``, ``deny_new``, ``one_more``);
* ``timeout_gate`` -- how the effective command wait is computed
  (``honor_cfg``, ``freeze``, ``shrink_now``);
* ``conn_return`` -- what happens when a pooled connection is released
  (``honor_cfg``, ``keep_out``, ``force_drop``).

An empty stack is, by construction, indistinguishable from the plain
API: every read returns the permanent configuration unchanged.
"""

from __future__ import annotations

import contextvars
import itertools
import typing


# ---------------------------------------------------------------------------
# Policy modes
# ---------------------------------------------------------------------------

#: Follow the permanent configuration and call arguments (stack neutral).
HONOR_CFG: typing.Final[str] = 'honor_cfg'

#: Reject every new statement / cursor fetch admitted in this frame.
DENY_NEW: typing.Final[str] = 'deny_new'

#: Admit exactly one more statement / cursor fetch in this frame.
ONE_MORE: typing.Final[str] = 'one_more'

#: Do not let a command timeout interrupt accepted executions.
FREEZE: typing.Final[str] = 'freeze'

#: Tighten the effective command wait to *timeout* seconds.
SHRINK_NOW: typing.Final[str] = 'shrink_now'

#: Do not hand released connections back to the pool in this frame.
KEEP_OUT: typing.Final[str] = 'keep_out'

#: Close the underlying connection whenever its release is requested.
FORCE_DROP: typing.Final[str] = 'force_drop'


_EXEC_ADMIT_MODES = frozenset({HONOR_CFG, DENY_NEW, ONE_MORE})
_TIMEOUT_GATE_MODES = frozenset({HONOR_CFG, FREEZE, SHRINK_NOW})
_CONN_RETURN_MODES = frozenset({HONOR_CFG, KEEP_OUT, FORCE_DROP})


_frame_counter = itertools.count(1)


class _Frame:
    __slots__ = (
        'id',
        'exec_admit',
        'timeout_gate',
        'conn_return',
        'timeout_cap',
    )

    def __init__(
        self,
        *,
        exec_admit: str,
        timeout_gate: str,
        conn_return: str,
        timeout_cap: typing.Optional[float],
    ) -> None:
        self.id = next(_frame_counter)
        self.exec_admit = exec_admit
        self.timeout_gate = timeout_gate
        self.conn_return = conn_return
        self.timeout_cap = timeout_cap


class _Scope:
    """Immutable snapshot of the stack plus per-frame admission counts.

    ``admissions`` is never mutated in place; admitting under a
    ``one_more`` frame derives a new scope, which keeps tasks that
    copied the context from leaking state back into their parent.
    """

    __slots__ = ('frames', 'admissions')

    def __init__(self, frames, admissions):
        self.frames = frames
        self.admissions = admissions


#: The current stack.  ``None`` means the stack is empty, i.e. the
#: permanent configuration applies unmodified.
_current: contextvars.ContextVar[typing.Optional[_Scope]] = \
    contextvars.ContextVar('asyncpg_execution_policy', default=None)


def _push(frame: _Frame):
    scope = _current.get()
    if scope is None:
        new_scope = _Scope((frame,), {})
    else:
        new_scope = _Scope(
            scope.frames + (frame,), scope.admissions)
    return _current.set(new_scope)


def _pop(token) -> None:
    _current.reset(token)


def _validate_timeout_cap(timeout) -> float:
    if timeout is None or isinstance(timeout, bool):
        raise ValueError(
            'invalid timeout value: expected non-negative float '
            '(got {!r})'.format(timeout))
    try:
        return float(timeout)
    except (TypeError, ValueError):
        raise ValueError(
            'invalid timeout value: expected non-negative float '
            '(got {!r})'.format(timeout)) from None


class policy_scope:
    """Push one frame onto the runtime execution-policy stack.

    Usable both as a synchronous and as an asynchronous context
    manager -- the frame itself never awaits::

        async with asyncpg.policy_scope(exec_admit='deny_new'):
            ...

        with asyncpg.policy_scope(timeout_gate='freeze'):
            ...

    :param exec_admit: ``'honor_cfg'`` (default), ``'deny_new'`` or
                       ``'one_more'``.
    :param timeout_gate: ``'honor_cfg'`` (default), ``'freeze'`` or
                         ``'shrink_now'``.
    :param conn_return: ``'honor_cfg'`` (default), ``'keep_out'`` or
                        ``'force_drop'``.
    :param timeout: Strict wait cap in seconds, required when
                    *timeout_gate* is ``'shrink_now'``.  Merged with
                    the connection's ``command_timeout`` and the
                    per-call timeout by taking the minimum; timeouts
                    keep surfacing through the regular error path.

    Frames are popped automatically, including when the body raises.
    """

    __slots__ = ('_frame', '_token')

    def __init__(
        self,
        *,
        exec_admit: str = HONOR_CFG,
        timeout_gate: str = HONOR_CFG,
        conn_return: str = HONOR_CFG,
        timeout: typing.Optional[float] = None,
    ) -> None:
        if exec_admit not in _EXEC_ADMIT_MODES:
            raise ValueError(
                'invalid exec_admit: expected one of {}, got {!r}'.format(
                    sorted(_EXEC_ADMIT_MODES), exec_admit))
        if timeout_gate not in _TIMEOUT_GATE_MODES:
            raise ValueError(
                'invalid timeout_gate: expected one of {}, got {!r}'.format(
                    sorted(_TIMEOUT_GATE_MODES), timeout_gate))
        if conn_return not in _CONN_RETURN_MODES:
            raise ValueError(
                'invalid conn_return: expected one of {}, got {!r}'.format(
                    sorted(_CONN_RETURN_MODES), conn_return))

        timeout_cap = None
        if timeout is not None:
            if timeout_gate is not SHRINK_NOW:
                raise ValueError(
                    "timeout is only accepted with "
                    "timeout_gate='shrink_now'")
            timeout_cap = _validate_timeout_cap(timeout)
        elif timeout_gate == SHRINK_NOW:
            raise ValueError(
                "timeout_gate='shrink_now' requires a timeout cap")

        self._frame = _Frame(
            exec_admit=exec_admit,
            timeout_gate=timeout_gate,
            conn_return=conn_return,
            timeout_cap=timeout_cap,
        )
        self._token = None

    def __enter__(self) -> 'policy_scope':
        self._token = _push(self._frame)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        token, self._token = self._token, None
        _pop(token)
        return False

    async def __aenter__(self) -> 'policy_scope':
        self._token = _push(self._frame)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        token, self._token = self._token, None
        _pop(token)
        return False


# ---------------------------------------------------------------------------
# Read side, used by Connection / Cursor / Pool
# ---------------------------------------------------------------------------

def admit_statement(operation: str) -> None:
    """Check whether a new statement / result fetch may be admitted.

    Reads the stack *now*; must be called at every user-facing dispatch
    exit, not cached by the caller.  An in-flight execution is never
    touched -- only statements that have not sent anything yet can be
    rejected.
    """
    scope = _current.get()
    if scope is None:
        return

    for frame in reversed(scope.frames):
        mode = frame.exec_admit
        if mode == HONOR_CFG:
            continue
        if mode == DENY_NEW:
            from . import exceptions
            raise exceptions.ExecutionAdmissionError(
                "exec_admit='deny_new' is active: {}() was rejected "
                "before any new statement was sent".format(operation))

        # one_more
        used = scope.admissions.get(frame.id, 0)
        if used >= 1:
            from . import exceptions
            raise exceptions.ExecutionAdmissionError(
                "exec_admit='one_more' is active and has already "
                "admitted one statement: {}() was rejected before any "
                "new statement was sent".format(operation))

        # Admit exactly one; derive a fresh scope so sibling
        # contexts/tasks observe their own counter.
        admissions = dict(scope.admissions)
        admissions[frame.id] = used + 1
        _current.set(_Scope(scope.frames, admissions))
        return


def resolve_command_timeout(merged_timeout):
    """Apply the timeout gate to a *merged* command timeout.

    ``merged_timeout`` is the wait computed from the connection's
    permanent ``command_timeout`` and the per-call argument (``None``
    means no deadline).  Returns the effective wait as a float, or
    ``None`` when a ``freeze`` frame removes the deadline altogether.
    Never mutates the connection's configured default.
    """
    scope = _current.get()
    if scope is None:
        return merged_timeout

    for frame in reversed(scope.frames):
        mode = frame.timeout_gate
        if mode == HONOR_CFG:
            continue
        if mode == FREEZE:
            return None
        # shrink_now
        cap = frame.timeout_cap
        if merged_timeout is None:
            return cap
        return min(merged_timeout, cap)

    return merged_timeout


def effective_conn_return() -> str:
    """Return the effective conn-return mode at this point in scope."""
    scope = _current.get()
    if scope is None:
        return HONOR_CFG

    for frame in reversed(scope.frames):
        if frame.conn_return != HONOR_CFG:
            return frame.conn_return
    return HONOR_CFG


class EffectivePolicy(typing.NamedTuple):
    """Observed, fully-resolved view of the current stack."""

    exec_admit: str
    timeout_gate: str
    conn_return: str
    shrink_timeout: typing.Optional[float]


def effective_policy() -> typing.Optional[EffectivePolicy]:
    """Return the resolved policy view, or ``None`` when the stack is empty.

    ``timeout_gate`` is reported as ``'honor_cfg'``, ``'freeze'`` or
    ``'shrink_now'``; ``shrink_timeout`` carries the strict cap for the
    latter.  Note that ``exec_admit='one_more'`` is reported as
    configured -- whether the one slot has already been consumed is an
    admission-counting detail internal to :func:`admit_statement`.
    """
    scope = _current.get()
    if scope is None:
        return None

    exec_admit = HONOR_CFG
    timeout_gate = HONOR_CFG
    shrink_timeout = None
    conn_return = HONOR_CFG

    for frame in reversed(scope.frames):
        if exec_admit == HONOR_CFG and frame.exec_admit != HONOR_CFG:
            exec_admit = frame.exec_admit
        if timeout_gate == HONOR_CFG and frame.timeout_gate != HONOR_CFG:
            timeout_gate = frame.timeout_gate
            shrink_timeout = frame.timeout_cap
        if conn_return == HONOR_CFG and frame.conn_return != HONOR_CFG:
            conn_return = frame.conn_return

    return EffectivePolicy(
        exec_admit=exec_admit,
        timeout_gate=timeout_gate,
        conn_return=conn_return,
        shrink_timeout=shrink_timeout,
    )
