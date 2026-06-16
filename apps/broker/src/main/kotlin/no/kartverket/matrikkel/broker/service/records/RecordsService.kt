package no.kartverket.matrikkel.broker.service.records

import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.insertRecord
import no.kartverket.matrikkel.kafkaclient.*
import javax.sql.DataSource

object Records {
    interface Service {
        suspend fun publish(topic: Topic, identity: ServiceIdentity, request: PublishRequest): Result<PublishResponse>
        suspend fun poll(topic: Topic, identity: ServiceIdentity, request: PollRequest): Result<PollResponse>
        suspend fun commit(topic: Topic, identity: ServiceIdentity, request: CommitRequest): Result<CommitResponse>
        suspend fun seek(topic: Topic, identity: ServiceIdentity, request: SeekRequest): Result<SeekResponse>
        suspend fun heartbeat(topic: Topic, identity: ServiceIdentity, request: HeartbeatRequest): Result<HeartbeatResponse>
    }

    val PublishLock = object : DbMutex.LockScope {
        override val seed: Long = 1231231L
    }

    class ServiceImpl(val dataSource: DataSource) : Service {
        override suspend fun publish(
            topic: Topic,
            identity: ServiceIdentity,
            request: PublishRequest
        ): Result<PublishResponse> {
            return dataSource.withTransaction {
                DbMutex.withLock(PublishLock, topic.name) {
                    val existing = findExistingPublishedRecord(topic, identity, request.idempotencyKey)
                    if (existing != null) {
                        Result.success(existing)
                    } else {
                        insertRecord(
                            topic = topic,
                            identity = identity,
                            request = request,
                            sequence = currentHeadForTopic(topic) + 1
                        )
                    }
                }
            }
        }

        override suspend fun poll(
            topic: Topic,
            identity: ServiceIdentity,
            request: PollRequest
        ): Result<PollResponse> {
            return Result.failure("poll to ${topic.name} by ${identity.value}")
        }

        override suspend fun commit(
            topic: Topic,
            identity: ServiceIdentity,
            request: CommitRequest
        ): Result<CommitResponse> {
            return Result.failure("commit to ${topic.name} by ${identity.value}")
        }

        override suspend fun seek(
            topic: Topic,
            identity: ServiceIdentity,
            request: SeekRequest
        ): Result<SeekResponse> {
            return Result.failure("seek to ${topic.name} by ${identity.value}")
        }

        override suspend fun heartbeat(
            topic: Topic,
            identity: ServiceIdentity,
            request: HeartbeatRequest
        ): Result<HeartbeatResponse> {
            return Result.failure("heartbeat to ${topic.name} by ${identity.value}")
        }
    }
}

private fun <T> Result.Companion.failure(error: String) = Result.failure<T>(Exception(error))