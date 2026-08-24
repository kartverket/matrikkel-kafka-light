package no.kartverket.matrikkel.kafkaclient

import kotlinx.serialization.Serializable
import kotlin.time.Instant

/**
 * Represents a request/responses going over-the-wire between the clients and the broker
 *
 * **NB!!** Might be useful to extract into separate package if we dont want the broker to
 * have a dependency to the client
 */

@Serializable
data class ErrorResponse(
    val code: String,
    val message: String,
)

@Serializable
data class PublishRequest(
    val idempotencyKey: String,
    val records: List<PublishRecord>,
)

@Serializable
data class PublishRecord(
    val key: ByteArray,
    val payload: ByteArray?,
)

@Serializable
data class PublishResponse(
    val topic: String,
    val sequence: Long,
    val idempotencyKey: String,
    val publishedAt: Instant,
)

@Serializable
enum class InitialOffsetPolicy {
    EARLIEST, LATEST
}

@Serializable
data class PollRequest (
    val maxRecords: Int,
    val consumerGroup: String,
    val instanceId: String,
    val initialOffsetPolicy: InitialOffsetPolicy,
)

@Serializable
data class PollRecord(
    val key: ByteArray,
    val payload: ByteArray?,
    val sequence: Long,
    val publishedAt: Instant,
)

@Serializable
data class PollResponse (
    val records: List<PollRecord>,
    val leaseToken: String,
)

@Serializable
data class CommitRequest(
    val leaseToken: String,
    val sequence: Long,
)

@Serializable
data class CommitResponse(
    val leaseToken: String,
)

@Serializable
data class SeekRequest(
    val consumerGroup: String,
    val sequence: Long,
)

@Serializable
class SeekResponse

@Serializable
class HeartbeatRequest

@Serializable
class HeartbeatResponse
