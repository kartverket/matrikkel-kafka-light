package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.findExistingPublishedRecord
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
        recordKey = "record_key",
        payload = "custom payload".toByteArray(),
    )


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
            currentHeadForTopic(topic)
        }

        assertThat(currentHead).isEqualTo(10L)
    }

    @Test
    fun `should return null if no previous equal message is published`(): Unit = runBlocking {
        val existingRecord: PublishResponse? = dataSource().withSession {
            findExistingPublishedRecord(topic, identity, idempotencyKey, "random")
        }

        assertThat(existingRecord).isNull()
    }

    @Test
    fun `should return published record on insert`(): Unit = runBlocking {
        val insertedRow = dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecords(
                    topic = topic,
                    identity = identity,
                    correlationId = Uuid.random(),
                    request = PublishRequest(
                        idempotencyKey = idempotencyKey,
                        records = listOf(record),
                    ),
                )
            }
        }

        assertThat(insertedRow).isSuccess().isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::recordKey).isEqualTo(record.recordKey)
            prop(PublishResponse::idempotencyKey).isEqualTo(idempotencyKey)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }

    @Test
    fun `should return failure on duplicate insert`(): Unit = runBlocking {
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecords(
                    topic, identity, Uuid.random(), PublishRequest(
                        idempotencyKey = idempotencyKey,
                        records = listOf(record),
                    )
                )
            }
        }

        val insertedRow = dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecords(topic, identity, Uuid.random(), PublishRequest(
                    idempotencyKey = idempotencyKey,
                    records = listOf(record),
                )
                )
            }
        }

        assertThat(insertedRow).isFailure()
    }

    @Test
    fun `should return existing record`(): Unit = runBlocking {
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecords(topic, identity, Uuid.random(), PublishRequest(
                    idempotencyKey = idempotencyKey,
                    records = listOf(record),
                )
                )
            }
        }

        val existingRecord: PublishResponse? = dataSource().withSession {
            findExistingPublishedRecord(topic, identity, idempotencyKey, record.recordKey)
        }

        assertThat(existingRecord).isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::recordKey).isEqualTo(record.recordKey)
            prop(PublishResponse::idempotencyKey).isEqualTo(idempotencyKey)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }
}