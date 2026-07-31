package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
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

    @Test
    fun `should create offset and return 0 for InitialOffsetPolicy EARLIEST`(): Unit = runBlocking {
        val initalOffsetPolicy = InitialOffsetPolicy.EARLIEST

        val offset = dataSource().withTransaction {
            OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
        }
        assertThat(offset).isEqualTo(0)
    }

    @Test
    fun `should create offset and return 10 for OffsetPolicy LATEST`(): Unit = runBlocking {

        // Insert 10 records
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                repeat(10) { no ->
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

        val initalOffsetPolicy = InitialOffsetPolicy.LATEST
        val offset = dataSource().withTransaction {
            OffsetRepository.getOffset(topic, consumerGroup, initalOffsetPolicy)
        }
        assertThat(offset).isEqualTo(10)

    }
}