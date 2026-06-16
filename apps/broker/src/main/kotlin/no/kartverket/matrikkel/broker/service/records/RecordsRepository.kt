package no.kartverket.matrikkel.broker.service.records

import kotliquery.Session
import kotliquery.queryOf
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import org.intellij.lang.annotations.Language
import kotlin.time.toKotlinInstant

object RecordsRepository {
    context(tx: Session)
    fun findExistingPublishedRecord(
        topic: Topic,
        identity: ServiceIdentity,
        idempotencyKey: String,
    ): PublishResponse? {
        @Language("SQL")
        val query = queryOf(
            """
            SELECT sequence, record_key, correlation_id, published_at
            FROM records
            WHERE topic = ? AND producer_identity = ? AND idempotency_key = ? 
        """.trimIndent(), topic.name, identity.value, idempotencyKey
        )
            .map {
                PublishResponse(
                    topic = topic.name,
                    sequence = it.long("sequence"),
                    recordKey = it.string("record_key"),
                    idempotencyKey = idempotencyKey,
                    correlationId = it.string("correlation_id"),
                    publishedAt = it.instant("published_at").toKotlinInstant()
                )
            }
            .asSingle

        return tx.run(query)
    }

    context(tx: Session)
    fun currentHeadForTopic(topic: Topic): Long {
        @Language("SQL")
        val query = queryOf("""
            SELECT sequence FROM records
            WHERE topic = ?
            ORDER BY sequence DESC
            LIMIT 1
        """.trimIndent(), topic.name)
            .map { it.long("sequence") }
            .asSingle

        return tx.run(query) ?: 0L
    }


    context(tx: Session)
    fun insertRecord(
        topic: Topic,
        identity: ServiceIdentity,
        request: PublishRequest,
        sequence: Long,
    ): PublishResponse? {
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
            ) ON CONFLICT (producer_identity, topic, idempotency_key) DO NOTHING
            RETURNING sequence, published_at
        """.trimIndent()

        val params = mapOf(
            "topic" to topic.name,
            "sequence" to sequence,
            "producer_identity" to identity.value,
            "record_key" to request.recordKey,
            "idempotency_key" to request.idempotencyKey,
            "correlation_id" to request.correlationId,
            "payload" to request.payload,
        )

        val query = queryOf(sql, params)
            .map {
                PublishResponse(
                    topic = topic.name,
                    sequence = it.long("sequence"),
                    recordKey = request.recordKey,
                    idempotencyKey = request.idempotencyKey,
                    correlationId = request.correlationId,
                    publishedAt = it.instant("published_at").toKotlinInstant()
                )
            }
            .asSingle

        return tx.run(query)
    }
}