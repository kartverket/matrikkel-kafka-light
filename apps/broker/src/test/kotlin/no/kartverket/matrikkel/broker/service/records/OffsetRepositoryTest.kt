package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isSuccess
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.acquireLease
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.uuid.Uuid

class OffsetRepositoryTest : WithDatabase {
    val TestLock: DbMutex.LockScope = object : DbMutex.LockScope {
        override val seed: Long = 27597612847L
    }

    val topic = Topic(
        name = "dummy_topic",
        acl = TopicAccessControlList(
            publishIdentities = setOf(TopicAccessControlList.WILDCARD),
            consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
        )
    )
    val identity = ServiceIdentity("fake-user")
    val idempotencyKey = "idempotency_key"
    val record = PublishRecord(
        key = "record_key".toByteArray(),
        payload = "custom payload".toByteArray(),
    )
    val consumerGroup = "dummy_consumer_group"
    val instanceId = "instance"

    @Test
    fun `should create offset and return 0 for InitialOffsetPolicy EARLIEST`(): Unit = runBlocking {
        createRecord(10)

        val initalOffsetPolicy = InitialOffsetPolicy.EARLIEST
        val offset = dataSource().withTransaction {
            withLease(topic, consumerGroup, instanceId) {
                OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
            }
        }

        assertThat(offset).isSuccess().isEqualTo(0)
    }

    @Test
    fun `should create offset and return 10 for OffsetPolicy LATEST`(): Unit = runBlocking {
        createRecord(10)

        val initalOffsetPolicy = InitialOffsetPolicy.LATEST
        val offset = dataSource().withTransaction {
            withLease(topic, consumerGroup, instanceId) {
                OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
            }
        }

        assertThat(offset).isSuccess().isEqualTo(10)
    }

    @Test
    fun `should create offset and return 0 for OffsetPolicy LATEST`(): Unit = runBlocking {
        val initalOffsetPolicy = InitialOffsetPolicy.LATEST
        val offset = dataSource().withTransaction {
            withLease(topic, consumerGroup, instanceId) {
                OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
            }
        }

        assertThat(offset).isSuccess().isEqualTo(0)
    }

    @Test
    fun `cannot acquire lease`(): Unit = runBlocking {
        val initalOffsetPolicy = InitialOffsetPolicy.LATEST
        val offset = dataSource().withTransaction {
            // Stealing the lease here
            acquireLease(topic, consumerGroup, "anotherInstance")

            withLease(topic, consumerGroup, instanceId) {
                OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
            }
        }

        assertThat(offset).isFailure()
            .given {
                assertThat(it).hasMessage("Could not acquire lease")
            }
    }

    private suspend fun createRecord(numRcords: Int) {
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                repeat(numRcords) { no ->
                    RecordsRepository.insertRecords(
                        topic = topic,
                        identity = identity,
                        correlationId = Uuid.random(),
                        request = PublishRequest(
                            idempotencyKey = idempotencyKey + no,
                            records = listOf(record),
                        ),
                    )
                }
            }
        }
    }
}