package no.kartverket.matrikkel.broker.service.cleanup

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
    ) {
        val totalDeleted: Int get() = deletedByCompact + deletedByRetention
    }

    interface Service {
        suspend fun cleanup(topic: Topic): Result
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

        override suspend fun cleanup(topic: Topic): Result {
            val now = clock.now()
            val compact = topic.cleanupPolicies.filterIsInstance<CleanupPolicy.Compact>().singleOrNull()
            val delete = topic.cleanupPolicies.filterIsInstance<CleanupPolicy.Delete>().singleOrNull()

            var deletedByCompact = 0
            var deletedByRetention = 0

            if (delete != null) {
                val cutoff = now - delete.retention
                deletedByRetention = deleteInBatches(topic) { retentionBatch(it, topic, cutoff) }
            }
            if (compact != null) {
                val cutoff = now - compact.minCompactionLag
                val tombstoneCutoff = now - compact.tombstoneRetention
                deletedByCompact = deleteInBatches(topic) { compactBatch(it, topic, cutoff, tombstoneCutoff) }
            }

            val result = Result(topic.name, deletedByCompact, deletedByRetention)
            if (result.totalDeleted > 0) {
                log.info(
                    "Cleanup topic={} deletedByCompact={} deletedByRetention={}",
                    topic.name, deletedByCompact, deletedByRetention
                )
            }
            return result
        }

        /** Runs batches, each in its own transaction holding only the cleanup lock, until a batch is not full. */
        private suspend fun deleteInBatches(topic: Topic, batch: context(TransactionalSession) (TransactionalSession) -> Int): Int {
            var total = 0
            while (true) {
                val deleted = dataSource.withTransaction { tx ->
                    DbMutex.withLock(CleanupLock, topic.name) { batch(tx) }
                }
                total += deleted
                if (deleted < batchSize) return total
            }
        }

        context(_: TransactionalSession)
        private fun compactBatch(
            tx: TransactionalSession,
            topic: Topic,
            cutoff: Instant,
            tombstoneCutoff: Instant
        ): Int {
            @Language("SQL")
            val sql = """
                DELETE FROM records
                WHERE (topic, sequence) IN (
                    SELECT r.topic, r.sequence
                    FROM records r
                    WHERE r.topic = :topic
                    AND published_at < :cutoff
                    AND sequence < (SELECT MAX(sequence) FROM records WHERE topic = :topic)
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
                        "cutoff" to cutoff.toOffsetDateTime(),
                        "tombstoneCutoff" to tombstoneCutoff.toOffsetDateTime(),
                        "batchSize" to batchSize,
                    )
                )
            )
        }

        context(_: TransactionalSession)
        private fun retentionBatch(tx: TransactionalSession, topic: Topic, cutoff: Instant): Int {
            @Language("SQL")
            val sql = """
                DELETE FROM records
                WHERE (topic, sequence) IN (
                    SELECT topic, sequence
                    FROM records
                    WHERE topic = :topic
                      AND published_at < :cutoff
                      AND sequence < (SELECT MAX(sequence) FROM records WHERE topic = :topic)
                    ORDER BY sequence
                    LIMIT :batchSize
                )
            """.trimIndent()
            return tx.update(
                queryOf(
                    sql,
                    mapOf(
                        "topic" to topic.name,
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
