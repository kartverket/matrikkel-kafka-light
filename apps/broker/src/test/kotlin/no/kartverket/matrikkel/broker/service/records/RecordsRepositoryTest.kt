package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSuccess
import assertk.assertions.prop
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
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.isApproxNow
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class RecordsRepositoryTest : WithDatabase {
    val TestLock : DbMutex.LockScope = object : DbMutex.LockScope {
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
    val request = PublishRequest(
        recordKey = "record_key",
        idempotencyKey = "idempotency_key",
        correlationId = Uuid.random(),
        payload = "custom payload".toByteArray()
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
                    RecordsRepository.insertRecord(topic, identity, request.copy(idempotencyKey = request.idempotencyKey + no))
                }
            }
            currentHeadForTopic(topic)
        }

        assertThat(currentHead).isEqualTo(10L)
    }

    @Test
    fun `should return null if no previous equal message is published`(): Unit = runBlocking {
        val existingRecord: PublishResponse? = dataSource().withSession {
            findExistingPublishedRecord(topic, identity, request.idempotencyKey)
        }

        assertThat(existingRecord).isNull()
    }

    @Test
    fun `should return published record on insert`(): Unit = runBlocking {
        val insertedRow = dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecord(topic, identity, request)
            }
        }

        assertThat(insertedRow).isSuccess().isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::recordKey).isEqualTo(request.recordKey)
            prop(PublishResponse::idempotencyKey).isEqualTo(request.idempotencyKey)
            prop(PublishResponse::correlationId).isEqualTo(request.correlationId)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }

    @Test
    fun `should return failure on duplicate insert`(): Unit = runBlocking {
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecord(topic, identity, request)
            }
        }

        val insertedRow = dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecord(topic, identity, request)
            }
        }

        assertThat(insertedRow).isFailure()
    }

    @Test
    fun `should return existing record`(): Unit = runBlocking {
        dataSource().withTransaction {
            DbMutex.withLock(TestLock, topic.name) {
                RecordsRepository.insertRecord(topic, identity, request)
            }
        }

        val existingRecord: PublishResponse? = dataSource().withSession {
            findExistingPublishedRecord(topic, identity, request.idempotencyKey)
        }

        assertThat(existingRecord).isNotNull().all {
            prop(PublishResponse::topic).isEqualTo(topic.name)
            prop(PublishResponse::sequence).isEqualTo(1)
            prop(PublishResponse::recordKey).isEqualTo(request.recordKey)
            prop(PublishResponse::idempotencyKey).isEqualTo(request.idempotencyKey)
            prop(PublishResponse::correlationId).isEqualTo(request.correlationId)
            prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
        }
    }
}