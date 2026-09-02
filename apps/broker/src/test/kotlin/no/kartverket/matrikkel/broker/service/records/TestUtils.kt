package no.kartverket.no.kartverket.matrikkel.broker.service.records

import kotliquery.queryOf
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.LeaseStatus
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.acquireLease
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.intellij.lang.annotations.Language
import kotlin.time.Clock
import kotlin.time.Instant

object TestUtils {
    suspend fun WithDatabase.createLease(
        topic: Topic,
        consumerGroup: String,
        time: Instant = Clock.System.now()
    ): LeaseRepository.Lease {
        return dataSource().withTransaction {
            val status = acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "other_instance_id",
                now = time,
            )
            when (status) {
                is LeaseStatus.Acquired -> status.lease
                else -> error("Could not create lease")
            }
        }
    }

    suspend fun WithDatabase.createOffset(
        topic: Topic,
        consumerGroup: String,
        value: Long,
    ) {
        return dataSource().withTransaction {
            LeaseRepository.withLease(topic, consumerGroup, "instanceID", fn = {
                OffsetRepository.getOffset(topic, consumerGroup, InitialOffsetPolicy.EARLIEST)
                OffsetRepository.setOffset(topic, consumerGroup, value)
            })
            releaseLease(topic)
        }
    }

    suspend fun WithDatabase.releaseLease(
        topic: Topic,
    ) {
        dataSource().withTransaction { tx ->
            @Language("SQL")
            val query = queryOf("DELETE FROM consumer_leases where topic = ?", topic.name)
                .asUpdate

            tx.run(query)
        }
    }
}