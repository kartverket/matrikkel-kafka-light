package no.kartverket.no.kartverket.matrikkel.broker.testutils

import kotliquery.Parameter
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.intellij.lang.annotations.Language
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import javax.sql.DataSource
import kotlin.time.Instant
import kotlin.time.toJavaInstant

/** A raw row in the `records` table. A `null` payload is a tombstone. */
data class RawRecord(
    val sequence: Long,
    val key: String,
    val publishedAt: Instant,
    val payload: String? = "payload",
    val producer: String = "producer-a",
)

/** Inserts rows directly, bypassing the publish path so tests control `published_at` and tombstones. */
fun DataSource.insertRawRecords(topic: String, rows: List<RawRecord>) {
    @Language("SQL")
    val sql = """
        INSERT INTO records (topic, sequence, producer_identity, record_key, idempotency_key, correlation_id, payload, published_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """.trimIndent()

    using(sessionOf(this)) { session ->
        session.batchPreparedStatement(
            sql,
            rows.map { row ->
                listOf(
                    topic,
                    row.sequence,
                    row.producer,
                    row.key.toByteArray(),
                    "idem-${row.sequence}",
                    UUID.randomUUID(),
                    Parameter(row.payload?.toByteArray(), ByteArray::class.java),
                    OffsetDateTime.ofInstant(row.publishedAt.toJavaInstant(), ZoneOffset.UTC),
                )
            }
        )
    }
}

fun DataSource.storedSequences(topic: String): List<Long> =
    using(sessionOf(this)) { session ->
        session.run(
            queryOf("SELECT sequence FROM records WHERE topic = ? ORDER BY sequence", topic)
                .map { it.long("sequence") }
                .asList
        )
    }
