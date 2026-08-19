package no.kartverket.no.kartverket.matrikkel.broker.service.records

import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.LeaseStatus
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.acquireLease
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
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
}