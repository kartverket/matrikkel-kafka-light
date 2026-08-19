package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotEmpty
import assertk.assertions.isNotNull
import assertk.assertions.isSuccess
import assertk.assertions.prop
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.ServiceException
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.LeaseStatus
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.acquireLease
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.no.kartverket.matrikkel.broker.service.records.TestUtils.createLease
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

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
        createLease(topic, consumerGroup, currentTime)

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
    fun `withLease should acquire lease that is not taken`(): Unit = runBlocking {
        val leaseResult = dataSource().withTransaction {
            withLease(topic, consumerGroup, "dummy_instance_id") {
                "Lease acquired"
            }
        }

        assertThat(leaseResult).isSuccess()
            .given {
                assertThat(it).isEqualTo("Lease acquired")
            }
    }

    @Test
    fun `withLease using leaseToken requires a lease to exist`(): Unit = runBlocking {
        val leaseResult = dataSource().withTransaction {
            withLease(topic, "leasetoken") {
                "Lease acquired"
            }
        }
        assertThat(leaseResult).isFailure()
    }

    @Test
    fun `withLease using expired leaseToken should fail`(): Unit = runBlocking {
        val lease = createLease(topic, consumerGroup, currentTime - 2.minutes)
        val leaseResult = dataSource().withTransaction {
            withLease(topic, leaseToken = lease.token) {
                "Lease acquired"
            }
        }
        assertThat(leaseResult).isFailure()
    }

    @Test
    fun `withLease using valid leaseToken should work`(): Unit = runBlocking {
        val lease = createLease(topic, consumerGroup, currentTime)
        val leaseResult = dataSource().withTransaction {
            withLease(topic, leaseToken = lease.token) {
                "Lease acquired"
            }
        }
        assertThat(leaseResult).isSuccess()
    }

    @Test
    fun `withLease should give failure if lease is taken`(): Unit = runBlocking {
        createLease(topic, consumerGroup, currentTime)

        val leaseResult = dataSource().withTransaction {
            withLease(topic, consumerGroup, "dummy_instance_id") {
                error("Should not acquire lease")
            }
        }

        assertThat(leaseResult).isFailure()
            .given {
                assertThat(it)
                    .isInstanceOf(ServiceException::class)
                    .all {
                        hasMessage("Could not acquire lease")
                        prop(ServiceException::status).isEqualTo(HttpStatusCode.Locked)
                    }
            }
    }


    @Test
    fun `should acquire lease that is expired`(): Unit = runBlocking {
        createLease(topic, consumerGroup,currentTime - 2.minutes)

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
}