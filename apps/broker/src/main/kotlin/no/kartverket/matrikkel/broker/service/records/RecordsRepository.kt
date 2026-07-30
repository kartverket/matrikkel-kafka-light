package no.kartverket.matrikkel.broker.service.records

import kotliquery.Session
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.DbMutex.DbLockAcquired
import no.kartverket.matrikkel.kafkaclient.PollRecords
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import org.intellij.lang.annotations.Language
import kotlin.time.Instant
import kotlin.time.toKotlinInstant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid

object RecordsRepository {
    context(tx: Session)
    fun findExistingPublishedRecord(
        topic: Topic,
        identity: ServiceIdentity,
        idempotencyKey: String,
        recordKey: ByteArray,
    ): PublishResponse? {
        @Language("SQL")
        val query = queryOf(
            """
            SELECT sequence, record_key, published_at
            FROM records
            WHERE topic = ? AND producer_identity = ? AND idempotency_key = ? AND record_key = ?
        """.trimIndent(), topic.name, identity.value, idempotencyKey, recordKey
        )
            .map {
                PublishResponse(
                    topic = topic.name,
                    sequence = it.long("sequence"),
                    idempotencyKey = idempotencyKey,
                    publishedAt = it.instant("published_at").toKotlinInstant()
                )
            }
            .asSingle

        return tx.run(query)
    }

    context(tx: Session)
    fun currentHeadForTopic(topic: Topic): Long {
        @Language("SQL")
        val query = queryOf(
            """
            SELECT sequence FROM records
            WHERE topic = ?
            ORDER BY sequence DESC
            LIMIT 1
        """.trimIndent(), topic.name
        )
            .map { it.long("sequence") }
            .asSingle

        return tx.run(query) ?: 0L
    }

    context(tx: TransactionalSession, _: DbLockAcquired)
    fun insertRecords(
        topic: Topic,
        identity: ServiceIdentity,
        correlationId: Uuid,
        request: PublishRequest,
        initialSequence: Long = currentHeadForTopic(topic) + 1,
    ): Result<PublishResponse> {
        @Language("SQL")
        val sql = """
            INSERT INTO records (
                topic,
                sequence,
                producer_identity,
                record_key,
                idempotency_key,
                correlation_id,
                payload,
                published_at
            ) VALUES (
                :topic,
                :sequence,
                :producer_identity,
                :record_key,
                :idempotency_key,
                :correlation_id,
                :payload,
                NOW()
            ) ON CONFLICT (topic, record_key, producer_identity, idempotency_key) DO NOTHING
        """.trimIndent()

        val lastRecord = request.records.last()
        var sequence = initialSequence
        val params = request.records.map { record ->
            mapOf(
                "topic" to topic.name,
                "sequence" to sequence++,
                "producer_identity" to identity.value,
                "record_key" to record.key,
                "idempotency_key" to request.idempotencyKey,
                "correlation_id" to correlationId.toJavaUuid(),
                "payload" to record.payload,
            )
        }


        return runCatching {
            val dbNow: Instant = requireNotNull(
                tx.single(queryOf("SELECT now()")) {
                    it.instant(1).toKotlinInstant()
                }
            )
            val result = tx.batchPreparedNamedStatement(
                sql,
                params,
            )
            require(result.sum() == request.records.size)

            PublishResponse(
                topic = topic.name,
                sequence = sequence - 1,
                idempotencyKey = request.idempotencyKey,
                publishedAt = dbNow,
            )
        }
    }

    context(tx: Session)
    fun pollRecords(
        topic: Topic,
        maxRecords: Int,
        offset: Long,
    ): List<PollRecords> {
        val paramMapPoll = mapOf(
            "topic" to topic.name,
            "sequence" to offset,
            "maxRecords" to maxRecords
        )

        @Language("SQL")
        val query = queryOf(
            """
            SELECT record_key, payload, sequence, published_at
            FROM records
            WHERE topic = :topic AND sequence > :sequence
            LIMIT :maxRecords
        """.trimIndent(), paramMapPoll
        )
            .map {
                PollRecords(
                    key = it.bytes("record_key"),
                    payload = it.bytes("payload"),
                    sequence = it.long("sequence"),
                    publishedAt = it.instant("published_at").toKotlinInstant()
                )
            }
            .asList

        return tx.run(query)
    }
}