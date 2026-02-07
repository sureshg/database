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

import org.funfix.delayedqueue.jvm.internals.jdbc.Migration

/** MariaDB-specific migrations for the DelayedQueue table. */
internal object MariaDBMigrations {
    /**
     * Gets the list of migrations for MariaDB.
     *
     * @param tableName The name of the delayed queue table
     * @return List of migrations in order
     */
    fun getMigrations(tableName: String): List<Migration> =
        listOf(
            Migration.createTableIfNotExists(
                tableName = tableName,
                sql =
                    """
                    CREATE TABLE `${tableName}` (
                        `id` BIGINT NOT NULL AUTO_INCREMENT,
                        `pKey` VARCHAR(200) NOT NULL,
                        `pKind` VARCHAR(100) NOT NULL,
                        `payload` LONGBLOB NOT NULL,
                        `scheduledAt` BIGINT NOT NULL,
                        `scheduledAtInitially` BIGINT NOT NULL,
                        `lockUuid` VARCHAR(36) NULL,
                        `createdAt` BIGINT NOT NULL,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `${tableName}__KindPlusKeyUniqueIndex` (`pKind`, `pKey`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

                    CREATE INDEX `${tableName}__KindPlusScheduledAtIndex` ON `$tableName`(`pKind`, `scheduledAt`);
                    CREATE INDEX `${tableName}__LockUuidPlusIdIndex` ON `$tableName`(`lockUuid`, `id`)
                    """,
            )
        )
}
