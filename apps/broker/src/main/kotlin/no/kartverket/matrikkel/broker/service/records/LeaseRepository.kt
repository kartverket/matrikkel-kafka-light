package no.kartverket.matrikkel.broker.service.records

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.kartverket.matrikkel.broker.ServiceException
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.isAfter
import no.kartverket.matrikkel.broker.isBefore
import no.kartverket.matrikkel.broker.repository.DbMutex
import org.intellij.lang.annotations.Language
import java.util.UUID
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.time.toJavaInstant

import kotlin.time.toKotlinInstant


object LeaseRepository {
    private val CreateLeaseEntryLock = object : DbMutex.LockScope {
        override val seed: Long = 79164871L
    }

    sealed interface LeaseStatus {
        data class Acquired(val lease: Lease) : LeaseStatus
        object Locked : LeaseStatus
    }

    data class Lease(
        val topic: String,
        val consumerGroup: String,
        val instanceId: String,
        val token: String,
        val expiresAt: Instant,
    )

    context(tx: TransactionalSession)
    fun <T> withLease(
        topic: Topic,
        consumerGroup: String,
        instanceId: String,
        now: Instant = Clock.System.now(),
        fn: context(LeaseStatus.Acquired) (Lease) -> T
    ): Result<T> {
        return when(val leaseStatus = acquireLease(topic, consumerGroup, instanceId, now)) {
            is LeaseStatus.Locked -> Result.failure(ServiceException.locked(message = "Could not acquire lease"))
            is LeaseStatus.Acquired -> {
                with(leaseStatus) {
                    runCatching {
                        fn(lease)
                    }
                }
            }
        }
    }

    context(tx: TransactionalSession)
    fun acquireLease(
        topic: Topic,
        consumerGroup: String,
        instanceId: String,
        now: Instant = Clock.System.now(),
    ): LeaseStatus {

        val paramMapTopicConsumer = mapOf(
            "topic" to topic.name,
            "consumer_group" to consumerGroup,
        )

        @Language("SQL")
        val query = queryOf(
            """
            SELECT topic, consumer_group,instance_id, token, expires_at
            FROM consumer_leases
            WHERE topic = :topic AND consumer_group = :consumer_group
            FOR UPDATE
        """.trimIndent(), paramMapTopicConsumer
        )
            .map {
                Lease(
                    topic = it.string("topic"),
                    consumerGroup = it.string("consumer_group"),
                    instanceId = it.string("instance_id"),
                    token = it.string("token"),
                    expiresAt = it.instant("expires_at").toKotlinInstant()
                )
            }
            .asSingle

        val lease: Lease? = tx.run(query)

        if (lease == null) {
            // For å sikre at bare en konsument har mulighet til å få en lease taes det en eksplisitt lås her
            // Dette fordi "SELECT ... FOR UPDATE" ikke kan fungere når det ikke eksisterer en rad i tabellen å låse på.
            DbMutex.lock(CreateLeaseEntryLock, topic.name + consumerGroup)

            @Language("SQL")
            val insertQuery = queryOf(
                """
                INSERT INTO consumer_leases(topic, consumer_group)
                VALUES(:topic, :consumer_group)
            """.trimIndent(), paramMapTopicConsumer
            ).asUpdate
            tx.run(insertQuery)

            // Raden som kan låses på er satt inn, så da kan vi kalle oss selv på nytt.
            return acquireLease(topic, consumerGroup, instanceId, now)
        }

        val leaseToken = when {
            lease.token.isEmpty() -> UUID.randomUUID().toString()
            lease.expiresAt.isAfter(now) -> {
                when (lease.instanceId == instanceId) {
                    true -> lease.token
                    false -> null
                }
            }
            lease.expiresAt.isBefore(now) -> UUID.randomUUID().toString()
            else -> error("Should never happen")
        }

        if (leaseToken == null) {
            return LeaseStatus.Locked
        }

        val newLease = Lease(
            topic = topic.name,
            consumerGroup = consumerGroup,
            instanceId = instanceId,
            token = leaseToken,
            expiresAt = now + topic.leaseTime
        )

        val paramMapLease = mapOf(
            "topic" to newLease.topic,
            "consumer_group" to newLease.consumerGroup,
            "instance_id" to newLease.instanceId,
            "token" to newLease.token,
            "expires_at" to newLease.expiresAt.toJavaInstant()
        )

        @Language("SQL")
        val updateQuery = queryOf(
            """
                UPDATE consumer_leases
                SET instance_id = :instance_id, token = :token, expires_at = :expires_at
                WHERE topic = :topic AND consumer_group = :consumer_group
            """.trimIndent(), paramMapLease
        ).asUpdate

        tx.run(updateQuery)
        return LeaseStatus.Acquired(newLease)
    }
}