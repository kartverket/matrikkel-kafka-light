package no.kartverket.matrikkel.broker.service.cleanup

import kotlinx.coroutines.runBlocking
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.time.ZoneOffset
import javax.sql.DataSource
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.time.toJavaInstant

object Cleanup {
    val CleanupLock = object : DbMutex.LockScope {
        override val seed: Long = 4564564L
    }

    data class Result(
        val topic: String,
        val deletedByCompact: Int,
        val deletedByRetention: Int,
        val maxCleanableSequence: Long,
    ) {
        val totalDeleted: Int get() = deletedByCompact + deletedByRetention
    }

    interface Service {
        fun cleanup(topic: Topic): Result
    }

    class ServiceImpl(
        private val dataSource: DataSource,
        private val batchSize: Int = 1000,
        private val clock: Clock = Clock.System,
    ) : Service {
        private val log = LoggerFactory.getLogger(Cleanup::class.java)

        init {
            require(batchSize > 0) { "batchSize must be positive" }
        }

        override fun cleanup(topic: Topic): Result {
            val now = clock.now()
            val compact = topic.cleanupPolicies.filterIsInstance<CleanupPolicy.Compact>().singleOrNull()
            val delete = topic.cleanupPolicies.filterIsInstance<CleanupPolicy.Delete>().singleOrNull()

            val maxCleanable = findMaxCleanableSequence(topic, now - topic.minCleanableAge)
            var deletedByCompact = 0
            var deletedByRetention = 0

            if (maxCleanable > 0) {
                if (delete != null) {
                    val cutoff = now - delete.retention
                    deletedByRetention = deleteInBatches(topic) { retentionBatch(it, topic, maxCleanable, cutoff) }
                }
                if (compact != null) {
                    val tombstoneCutoff = now - compact.tombstoneRetention
                    deletedByCompact = deleteInBatches(topic) { compactBatch(it, topic, maxCleanable, tombstoneCutoff) }
                }
            }

            val result = Result(topic.name, deletedByCompact, deletedByRetention, maxCleanable)
            if (result.totalDeleted > 0) {
                log.info(
                    "Cleanup topic={} deletedByCompact={} deletedByRetention={} maxCleanableSequence={}",
                    topic.name, deletedByCompact, deletedByRetention, maxCleanable
                )
            }
            return result
        }

        /** Runs batches, each in its own transaction holding only the cleanup lock, until a batch is not full. */
        private fun deleteInBatches(topic: Topic, batch: (TransactionalSession) -> Int): Int {
            var total = 0
            while (true) {
                val deleted = runBlocking {
                    dataSource.withTransaction { tx ->
                        DbMutex.withLock(CleanupLock, topic.name) { batch(tx) }
                    }
                }
                total += deleted
                if (deleted < batchSize) return total
            }
        }

        private fun findMaxCleanableSequence(topic: Topic, cutoff: Instant): Long = runBlocking {
            dataSource.withTransaction { tx ->
                @Language("SQL")
                val sql = """
                    SELECT COALESCE(MAX(sequence), 0) AS max_cleanable
                    FROM records
                    WHERE topic = :topic
                      AND published_at < :cutoff
                      AND sequence < (SELECT MAX(sequence) FROM records WHERE topic = :topic)
                """.trimIndent()
                tx.run(
                    queryOf(sql, mapOf("topic" to topic.name, "cutoff" to cutoff.toOffsetDateTime()))
                        .map { it.long("max_cleanable") }.asSingle
                ) ?: 0L
            }
        }

        private fun compactBatch(tx: TransactionalSession, topic: Topic, maxCleanable: Long, tombstoneCutoff: Instant): Int {
            @Language("SQL")
            val sql = """
                DELETE FROM records
                WHERE (topic, sequence) IN (
                    SELECT r.topic, r.sequence
                    FROM records r
                    WHERE r.topic = :topic
                      AND r.sequence <= :maxCleanable
                      AND (
                        EXISTS (
                            SELECT 1 FROM records n
                            WHERE n.topic = r.topic AND n.record_key = r.record_key AND n.sequence > r.sequence
                        )
                        OR (r.payload IS NULL AND r.published_at < :tombstoneCutoff)
                      )
                    ORDER BY r.sequence
                    LIMIT :batchSize
                )
            """.trimIndent()
            return tx.update(
                queryOf(
                    sql,
                    mapOf(
                        "topic" to topic.name,
                        "maxCleanable" to maxCleanable,
                        "tombstoneCutoff" to tombstoneCutoff.toOffsetDateTime(),
                        "batchSize" to batchSize,
                    )
                )
            )
        }

        private fun retentionBatch(tx: TransactionalSession, topic: Topic, maxCleanable: Long, cutoff: Instant): Int {
            @Language("SQL")
            val sql = """
                DELETE FROM records
                WHERE (topic, sequence) IN (
                    SELECT topic, sequence
                    FROM records
                    WHERE topic = :topic AND sequence <= :maxCleanable AND published_at < :cutoff
                    ORDER BY sequence
                    LIMIT :batchSize
                )
            """.trimIndent()
            return tx.update(
                queryOf(
                    sql,
                    mapOf(
                        "topic" to topic.name,
                        "maxCleanable" to maxCleanable,
                        "cutoff" to cutoff.toOffsetDateTime(),
                        "batchSize" to batchSize,
                    )
                )
            )
        }

        private fun Instant.toOffsetDateTime(): OffsetDateTime =
            OffsetDateTime.ofInstant(toJavaInstant(), ZoneOffset.UTC)
    }
}
