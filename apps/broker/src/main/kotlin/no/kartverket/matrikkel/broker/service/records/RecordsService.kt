package no.kartverket.matrikkel.broker.service.records

import kotliquery.TransactionalSession
import no.kartverket.matrikkel.broker.ServiceException
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.isAfter
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.matrikkel.broker.service.records.OffsetRepository.getOffset
import no.kartverket.matrikkel.broker.service.records.OffsetRepository.getOffsetOrNull
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.insertRecords
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.pollRecords
import no.kartverket.matrikkel.kafkaclient.*
import javax.sql.DataSource
import kotlin.time.Clock
import kotlin.uuid.Uuid

object Records {
    interface Service {
        data class Ctx(
            val topic: Topic,
            val identity: ServiceIdentity,
            val correlationId: Uuid,
        )

        suspend fun publish(ctx: Ctx, request: PublishRequest): Result<PublishResponse>
        suspend fun poll(ctx: Ctx, request: PollRequest): Result<PollResponse>
        suspend fun commit(ctx: Ctx, request: CommitRequest): Result<CommitResponse>
        suspend fun seek(ctx: Ctx, request: SeekRequest): Result<SeekResponse>
        suspend fun heartbeat(ctx: Ctx, request: HeartbeatRequest): Result<HeartbeatResponse>
    }

    val PublishLock = object : DbMutex.LockScope {
        override val seed: Long = 1231231L
    }

    class ServiceImpl(val dataSource: DataSource) : Service {
        override suspend fun publish(
            ctx: Service.Ctx,
            request: PublishRequest
        ): Result<PublishResponse> {
            return dataSource.withTransaction {
                DbMutex.withLock(PublishLock, ctx.topic.name) {
                    val lastRecord = request.records.last()
                    val existing =
                        findExistingPublishedRecord(ctx.topic, ctx.identity, request.idempotencyKey, lastRecord.key)
                    if (existing != null) {
                        Result.success(existing)
                    } else {
                        insertRecords(
                            topic = ctx.topic,
                            identity = ctx.identity,
                            correlationId = ctx.correlationId,
                            request = request,
                            initialSequence = currentHeadForTopic(ctx.topic)
                        )
                    }
                }
            }
        }

        override suspend fun poll(
            ctx: Service.Ctx,
            request: PollRequest
        ): Result<PollResponse> {
            return dataSource.withTransaction {
                withLease(ctx.topic, request.consumerGroup, request.instanceId) { lease ->
                    val offset = getOffset(ctx.topic, request.consumerGroup, request.initialOffsetPolicy)
                    val polledRecords = pollRecords(ctx.topic, request.maxRecords, offset)
                    PollResponse(polledRecords, lease.token)
                }
            }
        }

        override suspend fun commit(
            ctx: Service.Ctx,
            request: CommitRequest
        ): Result<CommitResponse> {
            return dataSource.withTransaction {
                withLease(ctx.topic, request.leaseToken) { lease ->
                    val currentOffset = getOffsetOrNull(ctx.topic, lease.consumerGroup)
                    if (currentOffset == null) {
                        throw ServiceException.badRequest(
                            code = "premature_commit",
                            message = "Cannot commit when offset does not exist. Have you polled before commiting?"
                        )
                    } else if (request.sequence < currentOffset) {
                        throw ServiceException.badRequest(
                            code = "invalid_offset",
                            message = "Offset must be larger then current offset"
                        )
                    } else {
                        OffsetRepository.setOffset(ctx.topic, request.sequence)
                        CommitResponse(leaseToken = lease.token)
                    }
                }
            }
        }

        override suspend fun seek(
            ctx: Service.Ctx,
            request: SeekRequest
        ): Result<SeekResponse> {
            return runCatching {
                dataSource.withTransaction {
                    require(request.sequence >= 0L) { "Cannot seek to any offset lower than 0" }
                    requireNoActiveLease(ctx.topic, request.consumerGroup)
                    requireSequenceNotAheadOfTopic(ctx.topic, request.sequence)

                    OffsetRepository.setOffset(ctx.topic, request.consumerGroup, request.sequence)
                    SeekResponse()
                }
            }
        }

        override suspend fun heartbeat(
            ctx: Service.Ctx,
            request: HeartbeatRequest
        ): Result<HeartbeatResponse> {
            return Result.failure("heartbeat to ${ctx.topic.name} by ${ctx.identity.value}")
        }


        context(tx: TransactionalSession)
        private fun requireNoActiveLease(
            topic: Topic,
            consumerGroup: String,
        ) {
            val lease = LeaseRepository.getLeaseForConsumerGroup(topic, consumerGroup)
            val validLease = when {
                lease == null -> false
                lease.token.isEmpty() -> false
                lease.expiresAt.isAfter(Clock.System.now()) -> false
                else -> true
            }

            if (validLease) {
                throw ServiceException.badRequest(
                    code = "active_lease",
                    message = "A current lease prevents seeking for this consumer group"
                )
            }
        }

        context(tx: TransactionalSession, leasestatus: LeaseRepository.LeaseStatus.Acquired)
        private fun requireSequenceNotLessThanCurrentOffset(topic: Topic, sequence: Long) {
            val currentOffset = getOffsetOrNull(topic, leasestatus.lease.consumerGroup)

            if (currentOffset == null) {
                throw ServiceException.badRequest(
                    code = "premature_commit",
                    message = "Cannot commit when offset does not exist. Have you polled before commiting?"
                )
            } else if (sequence < currentOffset) {
                throw ServiceException.badRequest(
                    code = "invalid_commit_sequence",
                    message = "Offset must be larger then current offset"
                )
            }
        }

        context(tx: TransactionalSession)
        private fun requireSequenceNotAheadOfTopic(topic: Topic, sequence: Long) {
            val topicHead = currentHeadForTopic(topic)
            if (topicHead < sequence) {
                throw ServiceException.badRequest(
                    code = "invalid_commit_sequence",
                    message = "Sequence must not be greater than the topic head"
                )
            }
        }
    }
}

private fun <T> Result.Companion.failure(error: String) = Result.failure<T>(Exception(error))