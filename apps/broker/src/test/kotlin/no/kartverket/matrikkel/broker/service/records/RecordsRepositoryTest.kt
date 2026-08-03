package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import kotlinx.coroutines.runBlocking
import kotliquery.TransactionalSession
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.pollRecords
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.isApproxNow
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class RecordsRepositoryTest : WithDatabase {
    val TestLock: DbMutex.LockScope = object : DbMutex.LockScope {
        override val seed: Long = 123L
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
    fun `should return zero for current head for empty topic`(): Unit = runBlocking {
        val currentHead: Long = dataSource().withSession {
            currentHeadForTopic(topic)
        }

        assertThat(currentHead).isEqualTo(0L)
    }

    @Test
    fun `should return last sequence for current head`(): Unit = runBlocking {
        val currentHead: Long = dataSource().withTransaction {
            repeat(10) { no -> insertRecord(idempotencyKey + no) }
            currentHeadForTopic(topic)
        }

        assertThat(currentHead).isEqualTo(10L)
    }

    @Test
    fun `should return null if no previous equal message is published`(): Unit = runBlocking {
        val existingRecord: PublishResponse? = dataSource().withSession {
            findExistingPublishedRecord(topic, identity, idempotencyKey, "random".toByteArray())
        }

        assertThat(existingRecord).isNull()
    }

    @Test
    fun `should return published record on insert`(): Unit = runBlocking {
        val insertedRow = dataSource().withTransaction {
            insertRecord()
        }

        assertThat(insertedRow).isSuccess().isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::idempotencyKey).isEqualTo(idempotencyKey)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }

    @Test
    fun `should return failure on duplicate insert`(): Unit = runBlocking {
        dataSource().withTransaction {
            insertRecord()
            val insertedRow = insertRecord()

            assertThat(insertedRow).isFailure()
        }
    }

    @Test
    fun `should return existing record`(): Unit = runBlocking {

        val existingRecord: PublishResponse? = dataSource().withTransaction {
            insertRecord()
            findExistingPublishedRecord(topic, identity, idempotencyKey, record.key)
        }

        assertThat(existingRecord).isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::idempotencyKey).isEqualTo(idempotencyKey)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }


    @Test
    fun `should return empty list`(): Unit = runBlocking {
        val polledRecords = dataSource().withTransaction {
            withLease(topic, consumerGroup, instanceId) {
                pollRecords(topic, 100, 0)
            }
        }

        assertThat(polledRecords).isSuccess().isEmpty()
    }

    @Test
    fun `should return first 10 records`(): Unit = runBlocking {
        val polledRecords = dataSource().withTransaction {
            repeat(50) { no -> insertRecord(idempotencyKey + no) }
            withLease(topic, consumerGroup, instanceId) {
                pollRecords(topic, 10, 0)
            }
        }

        assertThat(polledRecords).isSuccess()
            .given {
                val polledRecordsList = it
                assertThat(polledRecordsList).hasSize(10)
                assertThat(polledRecordsList.first().sequence).isEqualTo(1)
                assertThat(polledRecordsList.last().sequence).isEqualTo(10)
            }
    }

    @Test
    fun `should return last 10 records`(): Unit = runBlocking {
        val polledRecords = dataSource().withTransaction {
            repeat(50) { no -> insertRecord(idempotencyKey + no) }
            withLease(topic, consumerGroup, instanceId) {
                pollRecords(topic, 10, 40)
            }
        }

        assertThat(polledRecords).isSuccess()
            .given {
                val polledRecordsList = it
                assertThat(polledRecordsList).hasSize(10)
                assertThat(polledRecordsList.first().sequence).isEqualTo(41)
                assertThat(polledRecordsList.last().sequence).isEqualTo(50)
            }
    }

    @Test
    fun `should return below maxRecords when maxRecords is more than the number of records`(): Unit = runBlocking {
        val polledRecords = dataSource().withTransaction {
            repeat(5) { no -> insertRecord(idempotencyKey + no) }
            withLease(topic, consumerGroup, instanceId) {
                pollRecords(topic, 10, 0)
            }
        }

        assertThat(polledRecords).isSuccess()
            .given {
                val polledRecordsList = it
                assertThat(polledRecordsList).isNotEmpty()
                assertThat(polledRecordsList).hasSize(5)
                assertThat(polledRecordsList.first().sequence).isEqualTo(1)
                assertThat(polledRecordsList.last().sequence).isEqualTo(5)
            }
    }


    context(tx: TransactionalSession)
    private fun insertRecord(newIdempotencyKey: String = idempotencyKey): Result<PublishResponse> =
        DbMutex.withLock(TestLock, topic.name) {
            RecordsRepository.insertRecords(
                topic = topic,
                identity = identity,
                correlationId = Uuid.random(),
                request = PublishRequest(
                    idempotencyKey = newIdempotencyKey,
                    records = listOf(record),
                ),
            )
        }

}