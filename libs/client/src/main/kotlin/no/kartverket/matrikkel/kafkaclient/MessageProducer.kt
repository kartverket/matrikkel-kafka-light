package no.kartverket.matrikkel.kafkaclient

import java.io.Closeable

/**
 * This does not go over the wire since it carries type-information.
 * Thus, the client needs to map the records into a over-the-wire representation
 */
data class ProducerRecord<T>(
    val topic: String,
    val key: String,
    val idempotencyKey: String,
    val correlationId: String,
    val value: T? = null,
)

interface MessageProducer<T> : Closeable {
    suspend fun send(record: ProducerRecord<T>): PublishResponse
}