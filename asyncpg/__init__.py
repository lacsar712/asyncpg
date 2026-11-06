# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

from .connection import connect, Connection  # NOQA
from .exceptions import *  # NOQA
from .policy import (  # NOQA
    policy_scope,
    effective_policy,
    HONOR_CFG,
    DENY_NEW,
    ONE_MORE,
    FREEZE,
    SHRINK_NOW,
    KEEP_OUT,
    FORCE_DROP,
)
from .pool import create_pool, Pool  # NOQA
from .protocol import Record  # NOQA
from .types import *  # NOQA


from ._version import __version__  # NOQA

from . import exceptions  # NOQA
from . import policy  # NOQA


__all__: tuple[str, ...] = (
    'connect', 'create_pool', 'Pool', 'Record', 'Connection',
    'policy_scope', 'effective_policy',
    'HONOR_CFG', 'DENY_NEW', 'ONE_MORE', 'FREEZE', 'SHRINK_NOW',
    'KEEP_OUT', 'FORCE_DROP',
)
__all__ += exceptions.__all__ # NOQA
