package no.kartverket.matrikkel.kafkaclient

import kotlinx.serialization.Serializable
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * Represents a request/responses going over-the-wire between the clients and the broker
 *
 * **NB!!** Might be useful to extract into separate package if we dont want the broker to
 * have a dependency to the client
 */

@Serializable
data class PublishRequest(
    val idempotencyKey: String,
    val records: List<PublishRecord>,
)

@Serializable
data class PublishRecord(
    val recordKey: ByteArray,
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
class PollRequest

@Serializable
class PollResponse

@Serializable
class CommitRequest

@Serializable
class CommitResponse

@Serializable
class SeekRequest

@Serializable
class SeekResponse

@Serializable
class HeartbeatRequest

@Serializable
class HeartbeatResponse