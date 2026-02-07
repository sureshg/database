package org.funfix.delayedqueue.jvm.internals.utils

import java.sql.SQLException

/**
 * Test helper to call database APIs that declare context receivers for checked exceptions. It
 * supplies unsafe Raise contexts for InterruptedException and SQLException so tests can call
 * internal methods without changing production code.
 */
internal fun <T> sneakyRunDB(
    block:
        context(Raise<InterruptedException>, Raise<SQLException>)
        () -> T
): T = block(Raise._PRIVATE_AND_UNSAFE, Raise._PRIVATE_AND_UNSAFE)
