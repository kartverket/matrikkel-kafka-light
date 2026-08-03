package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotEmpty
import assertk.assertions.isNotNull
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.LeaseStatus
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.acquireLease
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class LeaseRepositoryTest : WithDatabase {
    val topic = Topic(
        name = "dummy_topic",
        acl = TopicAccessControlList(
            publishIdentities = setOf(TopicAccessControlList.WILDCARD),
            consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
        ),
        leaseTime = 30.seconds,
    )
    val consumerGroup = "dummy_consumer_group"
    val currentTime = Clock.System.now()


    @Test
    fun `should acquire new lease and renew lease`(): Unit = runBlocking {
        dataSource().withTransaction {

            val leaseStatus: LeaseStatus = acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "dummy_instance_id",
                now = currentTime,
            )

            var firstLeaseToken: String? = null
            assertThat(leaseStatus).isInstanceOf(LeaseStatus.Acquired::class)
                .given {
                    val lease = it.lease
                    assertThat(lease).isNotNull()
                    assertThat(lease.topic).isEqualTo(topic.name)
                    assertThat(lease.consumerGroup).isEqualTo(consumerGroup)
                    assertThat(lease.instanceId).isEqualTo("dummy_instance_id")
                    assertThat(lease.expiresAt).isEqualTo(currentTime + topic.leaseTime)
                    assertThat(lease.token).isNotEmpty()
                    firstLeaseToken = lease.token
                }

            val newLease: LeaseStatus = acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "dummy_instance_id",
                now = currentTime + 10.seconds,
            )
            assertThat(newLease).isInstanceOf(LeaseStatus.Acquired::class)
                .given {
                    val lease = it.lease
                    assertThat(lease.expiresAt).isEqualTo(currentTime + topic.leaseTime + 10.seconds)
                    assertThat(lease.token).isEqualTo(firstLeaseToken)
                }

        }

    }

    @Test
    fun `should not acquire lease that is taken`(): Unit = runBlocking {

        // Another instance acquires lease first
        createLease(currentTime)

        // Tries to acquire lease that is taken
        val leaseStatus: LeaseStatus = dataSource().withTransaction {
            acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "dummy_instance_id",
                now = currentTime,
            )
        }

        assertThat(leaseStatus).isInstanceOf(LeaseStatus.Locked::class)

    }

    @Test
    fun `should acquire lease that is expired`(): Unit = runBlocking {

        // Another instance acquires lease first
        createLease(currentTime - 2.minutes)

        // Tries to acquire lease that is taken
        val leaseStatus: LeaseStatus = dataSource().withTransaction {
            acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "dummy_instance_id",
                now = currentTime,
            )
        }

        assertThat(leaseStatus).isInstanceOf(LeaseStatus.Acquired::class)
    }


    private suspend fun createLease(time: Instant) {
        dataSource().withTransaction {
            acquireLease(
                topic = topic,
                consumerGroup = consumerGroup,
                instanceId = "other_instance_id",
                now = time,
            )
        }
    }
}