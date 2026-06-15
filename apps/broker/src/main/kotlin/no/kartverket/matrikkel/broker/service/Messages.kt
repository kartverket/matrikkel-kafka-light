package no.kartverket.matrikkel.broker.service

import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.utils.SealedResult
import no.kartverket.matrikkel.kafkaclient.*

object Messages {
    interface Service {
        fun publish(topic: Topic, identity: ServiceIdentity, request: PublishRequest): SealedResult<PublishResponse>
        fun poll(topic: Topic, identity: ServiceIdentity, request: PollRequest): SealedResult<PollResponse>
        fun commit(topic: Topic, identity: ServiceIdentity, request: CommitRequest): SealedResult<CommitResponse>
        fun seek(topic: Topic, identity: ServiceIdentity, request: SeekRequest): SealedResult<SeekResponse>
        fun heartbeat(topic: Topic, identity: ServiceIdentity, request: HeartbeatRequest): SealedResult<HeartbeatResponse>
    }

    class ServiceImpl : Service {
        // TODO Should probably defer implementation to a MessagesRepository
        override fun publish(
            topic: Topic,
            identity: ServiceIdentity,
            request: PublishRequest
        ): SealedResult<PublishResponse> {
            return SealedResult.failure("publish to ${topic.name} by ${identity.value}")
        }

        override fun poll(
            topic: Topic,
            identity: ServiceIdentity,
            request: PollRequest
        ): SealedResult<PollResponse> {
            return SealedResult.failure("poll to ${topic.name} by ${identity.value}")
        }

        override fun commit(
            topic: Topic,
            identity: ServiceIdentity,
            request: CommitRequest
        ): SealedResult<CommitResponse> {
            return SealedResult.failure("commit to ${topic.name} by ${identity.value}")
        }

        override fun seek(
            topic: Topic,
            identity: ServiceIdentity,
            request: SeekRequest
        ): SealedResult<SeekResponse> {
            return SealedResult.failure("seek to ${topic.name} by ${identity.value}")
        }

        override fun heartbeat(
            topic: Topic,
            identity: ServiceIdentity,
            request: HeartbeatRequest
        ): SealedResult<HeartbeatResponse> {
            return SealedResult.failure("heartbeat to ${topic.name} by ${identity.value}")
        }
    }
}