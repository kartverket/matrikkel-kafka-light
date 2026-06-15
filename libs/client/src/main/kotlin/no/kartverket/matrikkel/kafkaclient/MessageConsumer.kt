package no.kartverket.matrikkel.kafkaclient

import java.io.Closeable
import kotlin.time.Instant

/**
 * These do not go over the wire since they carry type-information.
 * Thus, the client needs to map the over-the-wire representation into ConsumerRecords
 */
data class ConsumerRecords<T>(
    val topic: String,
    val records: List<ConsumerRecord<T>>,
)
data class ConsumerRecord<T>(
    val topic: String,
    val sequence: Long,
    val key: String,
    val value: T? = null,
    val publishedAt: Instant,
)

interface MessageConsumer<T> : Closeable {
    suspend fun poll(maxRecords: Int): ConsumerRecords<T>
    suspend fun commitSync(sequence: Long): CommitResponse
    suspend fun seek(sequence: Long): ResetResponse
    suspend fun heartbeat(): HeartbeatResponse
}