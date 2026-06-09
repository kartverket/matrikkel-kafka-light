package no.kartverket.matrikkel.kafkaclient

import kotlinx.serialization.Serializable

/**
 * Represents a request/responses going over-the-wire between the clients and the broker
 *
 * **NB!!** Might be useful to extract into separate package if we dont want the broker to
 * have a dependency to the client
 */

@Serializable
class PublishRequest

@Serializable
class PublishResponse

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
class HeartbeatRequest

@Serializable
class HeartbeatResponse

@Serializable
class ResetRequest

@Serializable
class ResetResponse

