/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm.internals.jdbc.mariadb

import java.sql.SQLException
import org.funfix.delayedqueue.jvm.internals.jdbc.CommonSqlFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SqlExceptionFilter
import org.funfix.delayedqueue.jvm.internals.jdbc.matchesMessage

/** MariaDB-specific exception filters. */
internal object MariaDBFilters : RdbmsExceptionFilters {
    override val transientFailure: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    matchesTransient(e) -> true
                    matchesNextExceptionChain(e) -> true
                    else -> {
                        val cause = e.cause
                        cause != null && cause !== e && matches(cause)
                    }
                }
        }

    override val duplicateKey: SqlExceptionFilter =
        object : SqlExceptionFilter {
            override fun matches(e: Throwable): Boolean =
                when {
                    e is SQLException && e.errorCode == 1062 && e.sqlState == "23000" -> true
                    e is SQLException && matchesMessage(e.message, DUPLICATE_KEY_KEYWORDS) -> true
                    else -> false
                }
        }

    private val DUPLICATE_KEY_KEYWORDS =
        listOf("duplicate entry", "primary key constraint", "unique constraint")

    private fun matchesTransient(e: Throwable): Boolean =
        when {
            CommonSqlFilters.transactionTransient.matches(e) -> true
            e is SQLException && e.errorCode == 1213 -> true // Deadlock
            e is SQLException && e.errorCode == 1205 -> true // Lock wait timeout
            e is SQLException && e.errorCode == 1020 -> true // Record changed since last read
            else -> false
        }

    private fun matchesNextExceptionChain(e: Throwable): Boolean {
        val sqlException = e as? SQLException ?: return false
        var next = sqlException.nextException
        while (next != null && next !== e) {
            if (matchesTransient(next)) {
                return true
            }
            next = next.nextException
        }
        return false
    }
}
