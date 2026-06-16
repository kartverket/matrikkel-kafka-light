package no.kartverket.matrikkel.broker.service.records

import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.insertRecord
import no.kartverket.matrikkel.broker.utils.SealedResult
import no.kartverket.matrikkel.kafkaclient.*
import javax.sql.DataSource

object Records {
    interface Service {
        suspend fun publish(topic: Topic, identity: ServiceIdentity, request: PublishRequest): SealedResult<PublishResponse>
        suspend fun poll(topic: Topic, identity: ServiceIdentity, request: PollRequest): SealedResult<PollResponse>
        suspend fun commit(topic: Topic, identity: ServiceIdentity, request: CommitRequest): SealedResult<CommitResponse>
        suspend fun seek(topic: Topic, identity: ServiceIdentity, request: SeekRequest): SealedResult<SeekResponse>
        suspend fun heartbeat(topic: Topic, identity: ServiceIdentity, request: HeartbeatRequest): SealedResult<HeartbeatResponse>
    }

    val PublishLock = object : DbMutex.LockScope {
        override val seed: Long = 1231231L
    }

    class ServiceImpl(val dataSource: DataSource) : Service {
        override suspend fun publish(
            topic: Topic,
            identity: ServiceIdentity,
            request: PublishRequest
        ): SealedResult<PublishResponse> {
            return dataSource.withTransaction {
                DbMutex.lock(PublishLock, topic.name)
                val existing = findExistingPublishedRecord(topic, identity, request.idempotencyKey)
                if (existing != null) {
                    SealedResult.success(existing)
                } else {
                    val inserted: PublishResponse? = insertRecord(
                        topic = topic,
                        identity = identity,
                        request = request,
                        sequence = currentHeadForTopic(topic) + 1
                    )

                    if (inserted == null) {
                        SealedResult.failure("publishing of records failed")
                    } else {
                        SealedResult.success(inserted)
                    }
                }
            }
        }

        override suspend fun poll(
            topic: Topic,
            identity: ServiceIdentity,
            request: PollRequest
        ): SealedResult<PollResponse> {
            return SealedResult.failure("poll to ${topic.name} by ${identity.value}")
        }

        override suspend fun commit(
            topic: Topic,
            identity: ServiceIdentity,
            request: CommitRequest
        ): SealedResult<CommitResponse> {
            return SealedResult.failure("commit to ${topic.name} by ${identity.value}")
        }

        override suspend fun seek(
            topic: Topic,
            identity: ServiceIdentity,
            request: SeekRequest
        ): SealedResult<SeekResponse> {
            return SealedResult.failure("seek to ${topic.name} by ${identity.value}")
        }

        override suspend fun heartbeat(
            topic: Topic,
            identity: ServiceIdentity,
            request: HeartbeatRequest
        ): SealedResult<HeartbeatResponse> {
            return SealedResult.failure("heartbeat to ${topic.name} by ${identity.value}")
        }
    }
}