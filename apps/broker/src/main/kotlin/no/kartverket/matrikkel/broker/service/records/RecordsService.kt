package no.kartverket.matrikkel.broker.service.records

import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.insertRecords
import no.kartverket.matrikkel.kafkaclient.*
import javax.sql.DataSource
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
                    val existing = findExistingPublishedRecord(ctx.topic, ctx.identity, request.idempotencyKey, lastRecord.recordKey)
                    if (existing != null) {
                        Result.success(existing)
                    } else {
                        insertRecords(
                            topic = ctx.topic,
                            identity = ctx.identity,
                            correlationId = ctx.correlationId,
                            request = request,
                            initialSequence = currentHeadForTopic(ctx.topic) + 1
                        )
                    }
                }
            }
        }

        override suspend fun poll(
            ctx: Service.Ctx,
            request: PollRequest
        ): Result<PollResponse> {
            return Result.failure("poll to ${ctx.topic.name} by ${ctx.identity.value}")
        }

        override suspend fun commit(
            ctx: Service.Ctx,
            request: CommitRequest
        ): Result<CommitResponse> {
            return Result.failure("commit to ${ctx.topic.name} by ${ctx.identity.value}")
        }

        override suspend fun seek(
            ctx: Service.Ctx,
            request: SeekRequest
        ): Result<SeekResponse> {
            return Result.failure("seek to ${ctx.topic.name} by ${ctx.identity.value}")
        }

        override suspend fun heartbeat(
            ctx: Service.Ctx,
            request: HeartbeatRequest
        ): Result<HeartbeatResponse> {
            return Result.failure("heartbeat to ${ctx.topic.name} by ${ctx.identity.value}")
        }
    }
}

private fun <T> Result.Companion.failure(error: String) = Result.failure<T>(Exception(error))