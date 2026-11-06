# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

import asyncpg
from asyncpg import policy as apg_policy
from asyncpg import _testbase as tb


# ---------------------------------------------------------------------------
# Pure stack semantics -- no PostgreSQL server is required.
# ---------------------------------------------------------------------------


class TestPolicyStack(tb.TestCase):

    def test_policy_empty_stack_is_honor_cfg(self):
        self.assertIsNone(apg_policy.effective_policy())
        # An empty stack admits everything and leaves timeouts alone.
        apg_policy.admit_statement('execute')
        self.assertIsNone(apg_policy.resolve_command_timeout(None))
        self.assertEqual(
            apg_policy.resolve_command_timeout(0.5), 0.5)
        self.assertEqual(
            apg_policy.effective_conn_return(), apg_policy.HONOR_CFG)

    def test_policy_scope_push_pop(self):
        with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
            view = apg_policy.effective_policy()
            self.assertEqual(view.exec_admit, apg_policy.DENY_NEW)
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                apg_policy.admit_statement('fetch')
        self.assertIsNone(apg_policy.effective_policy())
        # The frame was popped, admission works again.
        apg_policy.admit_statement('fetch')

    def test_policy_scope_pops_on_exception(self):
        with self.assertRaises(ZeroDivisionError):
            with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
                raise ZeroDivisionError
        self.assertIsNone(apg_policy.effective_policy())

    def test_policy_inner_pop_restores_outer(self):
        with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                apg_policy.admit_statement('execute')
            with apg_policy.policy_scope(
                    exec_admit=apg_policy.HONOR_CFG,
                    conn_return=apg_policy.FORCE_DROP):
                # honor_cfg on exec_admit looks through to the outer
                # deny_new, while conn_return is overridden.
                view = apg_policy.effective_policy()
                self.assertEqual(view.exec_admit, apg_policy.DENY_NEW)
                self.assertEqual(view.conn_return, apg_policy.FORCE_DROP)
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    apg_policy.admit_statement('execute')
            # Inner frame gone: outer deny survives.
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                apg_policy.admit_statement('execute')
        self.assertIsNone(apg_policy.effective_policy())

    def test_policy_one_more_admits_exactly_one(self):
        with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
            apg_policy.admit_statement('execute')  # the single one
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                apg_policy.admit_statement('fetch')
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                apg_policy.admit_statement('fetch')
        # A fresh frame gets a fresh allowance.
        with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
            apg_policy.admit_statement('fetch')

    def test_policy_one_more_rereads_current_top(self):
        # An outer deny must not be "remembered" while an inner
        # one_more frame is active; the second statement re-reads the
        # stack and finds one_more exhausted.
        with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
            with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
                apg_policy.admit_statement('execute')
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    apg_policy.admit_statement('execute')

    def test_policy_timeout_gate(self):
        # freeze removes every deadline.
        with apg_policy.policy_scope(timeout_gate=apg_policy.FREEZE):
            self.assertIsNone(
                apg_policy.resolve_command_timeout(0.1))
            self.assertIsNone(
                apg_policy.resolve_command_timeout(None))

        # shrink_now tightens without ever loosening.
        with apg_policy.policy_scope(
                timeout_gate=apg_policy.SHRINK_NOW, timeout=0.05):
            self.assertEqual(
                apg_policy.resolve_command_timeout(0.5), 0.05)
            self.assertEqual(
                apg_policy.resolve_command_timeout(0.01), 0.01)
            self.assertEqual(
                apg_policy.resolve_command_timeout(None), 0.05)

        with apg_policy.policy_scope(timeout_gate=apg_policy.HONOR_CFG):
            self.assertEqual(
                apg_policy.resolve_command_timeout(0.5), 0.5)

    def test_policy_invalid_arguments(self):
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(exec_admit='nope')
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(timeout_gate='nope')
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(conn_return='nope')
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(
                timeout_gate=apg_policy.SHRINK_NOW)
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(
                timeout_gate=apg_policy.FREEZE, timeout=1.0)
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(timeout='soon')
        with self.assertRaises(ValueError):
            apg_policy.policy_scope(timeout=False)

    def test_policy_concurrent_scopes_do_not_cross(self):

        async def scenario():
            async def deny_side(ready):
                with apg_policy.policy_scope(
                        exec_admit=apg_policy.DENY_NEW):
                    ready.set()
                    await asyncio.sleep(0.05)
                    with self.assertRaises(
                            asyncpg.ExecutionAdmissionError):
                        apg_policy.admit_statement('execute')

            async def one_more_side(ready):
                await ready.wait()
                with apg_policy.policy_scope(
                        exec_admit=apg_policy.ONE_MORE):
                    apg_policy.admit_statement('execute')
                    await asyncio.sleep(0.05)
                    with self.assertRaises(
                            asyncpg.ExecutionAdmissionError):
                        apg_policy.admit_statement('execute')

            ready = asyncio.Event()
            # Run as tasks so each gets its own copied context; frames
            # pushed in one task must never surface in the other.
            t1 = asyncio.ensure_future(deny_side(ready))
            t2 = asyncio.ensure_future(one_more_side(ready))
            await asyncio.gather(t1, t2)
            # Neither task leaked a frame into the parent scope.
            self.assertIsNone(apg_policy.effective_policy())

        self.loop.run_until_complete(scenario())

    def test_policy_child_task_keeps_own_one_more_count(self):

        async def scenario():
            with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
                async def child():
                    # The copied context sees one_more, but consuming
                    # the allowance here must not consume it for the
                    # parent.
                    apg_policy.admit_statement('execute')
                    with self.assertRaises(
                            asyncpg.ExecutionAdmissionError):
                        apg_policy.admit_statement('execute')

                await asyncio.create_task(child())
                # Parent's allowance is untouched.
                apg_policy.admit_statement('execute')
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    apg_policy.admit_statement('execute')

        self.loop.run_until_complete(scenario())


# ---------------------------------------------------------------------------
# Wired behaviour -- requires a running PostgreSQL cluster.
# ---------------------------------------------------------------------------


class TestPolicyConnection(tb.ConnectedTestCase):

    async def test_deny_new_all_exits(self):
        exits = (
            lambda c: c.execute('SELECT 1'),
            lambda c: c.executemany(
                'SELECT $1::int', [(1,), (2,)]),
            lambda c: c.fetch('SELECT 1'),
            lambda c: c.fetchrow('SELECT 1'),
            lambda c: c.fetchval('SELECT 1'),
        )
        # Baseline: every exit works.
        for call in exits:
            await call(self.con)

        stmt = await self.con.prepare('SELECT 1')

        with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
            for call in exits:
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    await call(self.con)
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                await stmt.fetch()
            # Nothing was sent: the connection is immediately usable
            # by a scope without a frame.
            self.assertEqual(await self.con.fetchval('SELECT 42'), 42)

    async def test_deny_new_does_not_touch_in_flight(self):
        # An execution already on the wire must run to completion; a
        # deny pushed afterwards only blocks subsequent statements.
        task = self.loop.create_task(
            self.con.fetchval('SELECT pg_sleep(0.5)'))
        await asyncio.sleep(0.1)
        with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
            # The sleeping statement is not cancelled or faked: it
            # returns its real (NULL) result.
            self.assertIsNone(await task)
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                await self.con.fetchval('SELECT 1')
        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_one_more_connection(self):
        with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)
            with self.assertRaises(asyncpg.ExecutionAdmissionError):
                await self.con.fetchval('SELECT 2')

    async def test_other_connection_unaffected(self):
        other = await self.connect()
        try:
            async def denied():
                with apg_policy.policy_scope(
                        exec_admit=apg_policy.DENY_NEW):
                    await asyncio.sleep(0.05)
                    with self.assertRaises(
                            asyncpg.ExecutionAdmissionError):
                        await self.con.fetchval('SELECT 1')

            task = self.loop.create_task(denied())
            await asyncio.sleep(0.01)
            # Same event loop, a connection whose scope never pushed
            # anything keeps following the plain API.
            self.assertEqual(await other.fetchval('SELECT 1'), 1)
            await task
        finally:
            await other.close()

    async def test_freeze_waits_past_command_timeout(self):
        con = await self.connect(command_timeout=0.2)
        try:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.6):
                await con.fetchval('SELECT pg_sleep(1)')

            with apg_policy.policy_scope(timeout_gate=apg_policy.FREEZE):
                with self.assertRunUnder(1.5):
                    self.assertIsNone(
                        await con.fetchval('SELECT pg_sleep(0.5)'))
                # Simple-query path (no args) is frozen too.
                await con.execute('SELECT pg_sleep(0.5)')

            # Frame popped: the permanent timeout applies again.
            with self.assertRaises(asyncio.TimeoutError):
                await con.fetchval('SELECT pg_sleep(1)')
        finally:
            await con.close()

    async def test_freeze_does_not_swallow_real_errors(self):
        con = await self.connect(command_timeout=0.2)
        try:
            with apg_policy.policy_scope(timeout_gate=apg_policy.FREEZE):
                with self.assertRaises(asyncpg.PostgresError):
                    await con.fetchval(
                        "SELECT CAST('nope' AS integer)")
                with self.assertRaises(asyncpg.PostgresError):
                    await con.execute(
                        "SELECT CAST('nope' AS integer)")
        finally:
            await con.close()

    async def test_shrink_now_tightens_wait(self):
        con = await self.connect(command_timeout=60)
        try:
            # No frame: the generous configured timeout wins.
            with self.assertRunUnder(0.6):
                with self.assertRaises(asyncio.TimeoutError):
                    await con.fetchval('SELECT pg_sleep(1)', timeout=0.2)

            with apg_policy.policy_scope(
                    timeout_gate=apg_policy.SHRINK_NOW, timeout=0.2):
                with self.assertRunUnder(0.6):
                    with self.assertRaises(asyncio.TimeoutError):
                        await con.fetchval('SELECT pg_sleep(1)')
                # The cap tightens but never loosens the per-call wait.
                self.assertEqual(
                    await con.fetchval(
                        'SELECT pg_sleep(0.05)', timeout=5), None)
        finally:
            await con.close()

    async def test_configuration_is_not_mutated(self):
        con = await self.connect(command_timeout=0.3)
        try:
            with apg_policy.policy_scope(timeout_gate=apg_policy.FREEZE):
                await con.fetchval('SELECT 1')
            self.assertEqual(con._config.command_timeout, 0.3)
            with apg_policy.policy_scope(
                    timeout_gate=apg_policy.SHRINK_NOW, timeout=0.05):
                with self.assertRaises(asyncio.TimeoutError):
                    await con.fetchval('SELECT pg_sleep(1)')
            self.assertEqual(con._config.command_timeout, 0.3)
        finally:
            await con.close()


class TestPolicyCursor(tb.ConnectedTestCase):

    async def test_deny_new_on_cursor_iterator(self):
        async with self.con.transaction():
            cur = self.con.cursor(
                'SELECT i FROM generate_series(1, 5) AS i',
                prefetch=2,
            )
            it = cur.__aiter__()
            self.assertEqual(await it.__anext__(), (1,))
            with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
                # The already-materialized buffer keeps flowing...
                self.assertEqual(await it.__anext__(), (2,))
                # ...but the next server roundtrip is observably
                # rejected instead of reporting a fake completion.
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    await it.__anext__()

    async def test_deny_new_on_cursor_await_and_fetch(self):
        async with self.con.transaction():
            with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    await self.con.cursor(
                        'SELECT generate_series(1, 3)')

            cur = await self.con.cursor(
                'SELECT i FROM generate_series(1, 5) AS i')
            with apg_policy.policy_scope(exec_admit=apg_policy.DENY_NEW):
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    await cur.fetch(2)
            rows = await cur.fetch(2)
            self.assertEqual(len(rows), 2)

    async def test_one_more_cursor(self):
        async with self.con.transaction():
            cur = self.con.cursor(
                'SELECT i FROM generate_series(1, 5) AS i')
            with apg_policy.policy_scope(exec_admit=apg_policy.ONE_MORE):
                rows = await cur.fetch(2)
                self.assertEqual(len(rows), 2)
                with self.assertRaises(asyncpg.ExecutionAdmissionError):
                    await cur.fetch(2)

    async def test_freeze_cursor(self):
        con = await self.connect(command_timeout=0.2)
        try:
            async with con.transaction():
                cur = con.cursor('SELECT pg_sleep(0.5)')
                with apg_policy.policy_scope(timeout_gate=apg_policy.FREEZE):
                    with self.assertRunUnder(1.0):
                        rows = await cur.fetch(1)
                    self.assertEqual(len(rows), 1)
        finally:
            await con.close()


class TestPolicyPool(tb.ClusterTestCase):

    async def test_keep_out_blocks_return(self):
        pool = self.create_pool(
            min_size=1, max_size=1,
            max_inactive_connection_lifetime=0,
        )
        try:
            proxy = await pool.acquire()
            underlying = proxy._con
            size_before = pool.get_size()
            idle_before = pool.get_idle_size()

            with apg_policy.policy_scope(conn_return=apg_policy.KEEP_OUT):
                with self.assertRaises(
                        asyncpg.ConnectionReturnRejectedError):
                    await pool.release(proxy)

            # Still occupied: nothing was silently returned.
            self.assertFalse(underlying.is_closed())
            self.assertEqual(pool.get_size(), size_before)
            self.assertEqual(pool.get_idle_size(), idle_before)
            self.assertIsNotNone(proxy._con)

            # ...and a saturated pool cannot hand out a phantom conn.
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(pool.acquire(), timeout=0.2)

            # Once the frame is gone, the plain return works.
            await pool.release(proxy)
            self.assertEqual(pool.get_idle_size(), 1)
        finally:
            pool.terminate()

    async def test_keep_out_context_exit_is_observable(self):
        pool = self.create_pool(
            min_size=1, max_size=1,
            max_inactive_connection_lifetime=0,
        )
        try:
            held = []
            with self.assertRaises(asyncpg.ConnectionReturnRejectedError):
                async with pool.acquire() as con:
                    await con.fetchval('SELECT 1')
                    held.append(con)
                    with apg_policy.policy_scope(
                            conn_return=apg_policy.KEEP_OUT):
                        pass
            # The context-exit failure surface did not recycle the
            # connection; releasing after pop still works.
            self.assertEqual(pool.get_idle_size(), 0)
            await pool.release(held[0])
            self.assertEqual(pool.get_idle_size(), 1)
        finally:
            pool.terminate()

    async def test_force_drop_closes_underlying(self):
        pool = self.create_pool(
            min_size=1, max_size=2,
            max_inactive_connection_lifetime=0,
        )
        try:
            proxy = await pool.acquire()
            underlying = proxy._con
            self.assertFalse(underlying.is_closed())

            with apg_policy.policy_scope(conn_return=apg_policy.FORCE_DROP):
                await pool.release(proxy)

            # The underlying connection is observably gone...
            self.assertTrue(underlying.is_closed())
            self.assertIsNone(proxy._con)
            # ...but results the caller already holds are untouched.
            proxy2 = await pool.acquire()
            try:
                self.assertEqual(
                    await proxy2.fetchval('SELECT 1'), 1)
            finally:
                await pool.release(proxy2)
        finally:
            pool.terminate()

    async def test_keep_out_read_after_blocking_started(self):
        # A keep_out frame pushed *while release is already running*
        # (here: from within the user `reset` callback, which is
        # awaited on the release path) must be honored at the recycle
        # point instead of silently using the frame seen on entry.
        cm = apg_policy.policy_scope(conn_return=apg_policy.KEEP_OUT)
        pushed = False

        async def reset_push(con):
            # Push from the release path itself; the frame stays
            # active for the subsequent recycle decision. Only do it
            # once, so the later plain release after pop is unaffected.
            nonlocal pushed
            if not pushed:
                pushed = True
                cm.__enter__()
            await asyncio.sleep(0)

        pool = self.create_pool(
            min_size=1, max_size=1,
            max_inactive_connection_lifetime=0,
            reset=reset_push,
        )
        try:
            proxy = await pool.acquire()
            try:
                with self.assertRaises(
                        asyncpg.ConnectionReturnRejectedError):
                    await pool.release(proxy)
                self.assertEqual(pool.get_idle_size(), 0)
            finally:
                cm.__exit__(None, None, None)
            # Frame popped: the same holder returns normally now.
            await pool.release(proxy)
            self.assertEqual(pool.get_idle_size(), 1)
        finally:
            pool.terminate()

    async def test_concurrent_scopes_keep_returns_isolated(self):
        pool = self.create_pool(
            min_size=2, max_size=2,
            max_inactive_connection_lifetime=0,
        )
        try:
            proxy = await pool.acquire()
            underlying = proxy._con

            async def force_side():
                with apg_policy.policy_scope(
                        conn_return=apg_policy.FORCE_DROP):
                    await pool.release(proxy)

            async def acquire_other():
                async with pool.acquire() as other:
                    self.assertEqual(
                        await other.fetchval('SELECT 1'), 1)

            await asyncio.gather(force_side(), acquire_other())
            self.assertTrue(underlying.is_closed())
        finally:
            pool.terminate()
