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
        val lease: Lease? = getLeaseForConsumerGroup(topic, consumerGroup)

        if (lease == null) {
            // For å sikre at bare en konsument har mulighet til å få en lease taes det en eksplisitt lås her
            // Dette fordi "SELECT ... FOR UPDATE" ikke kan fungere når det ikke eksisterer en rad i tabellen å låse på.
            DbMutex.lock(CreateLeaseEntryLock, topic.name + consumerGroup)
            createLease(topic, consumerGroup)

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

        updateLease(newLease)

        return LeaseStatus.Acquired(newLease)
    }

    context(tx: TransactionalSession)
    private fun getLeaseForConsumerGroup(topic: Topic, consumerGroup: String): Lease? {
        @Language("SQL")
        val query = queryOf(
            """
                SELECT topic, consumer_group, instance_id, token, expires_at
                FROM consumer_leases
                WHERE topic = ? AND consumer_group = ?
                FOR UPDATE
            """.trimIndent(),
            topic.name, consumerGroup
        )
            .map(::leaseMapper)
            .asSingle

        return tx.run(query)
    }

    context(tx: TransactionalSession)
    private fun getLeaseForLeaseToken(topic: Topic, leaseToken: String): Lease? {
        @Language("SQL")
        val query = queryOf(
            """
                SELECT topic, consumer_group, instance_id, token, expires_at
                FROM consumer_leases
                WHERE topic = ? AND token = ?
                FOR UPDATE
            """.trimIndent(),
            topic.name, leaseToken
        )
            .map(::leaseMapper)
            .asSingle

        return tx.run(query)
    }

    context(tx: TransactionalSession)
    private fun createLease(topic: Topic, consumerGroup: String): Int {
        @Language("SQL")
        val query = queryOf(
            """
                INSERT INTO consumer_leases(topic, consumer_group)
                VALUES(?, ?)
            """.trimIndent(),
            topic.name, consumerGroup
        ).asUpdate

        return tx.run(query)
    }

    context(tx: TransactionalSession)
    private fun updateLease(lease: Lease): Int {
        val paramMapLease = mapOf(
            "topic" to lease.topic,
            "consumer_group" to lease.consumerGroup,
            "instance_id" to lease.instanceId,
            "token" to lease.token,
            "expires_at" to lease.expiresAt.toJavaInstant()
        )

        @Language("SQL")
        val updateQuery = queryOf(
            """
                UPDATE consumer_leases
                SET instance_id = :instance_id, token = :token, expires_at = :expires_at
                WHERE topic = :topic AND consumer_group = :consumer_group
            """.trimIndent(), paramMapLease
        ).asUpdate

        return tx.run(updateQuery)
    }


    private fun leaseMapper(row: Row): Lease {
        return Lease(
            topic = row.string("topic"),
            consumerGroup = row.string("consumer_group"),
            instanceId = row.string("instance_id"),
            token = row.string("token"),
            expiresAt = row.instant("expires_at").toKotlinInstant()
        )
    }
}