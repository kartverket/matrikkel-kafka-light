package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isLessThan
import assertk.assertions.isSuccess
import assertk.assertions.prop
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.isApproxNow
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime
import kotlin.uuid.Uuid

class RecordsServiceTest : WithDatabase {
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
    fun `should return existing published record if found`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val first = service.publish(topic, identity, request)

        delay(1.seconds)

        val second = service.publish(topic, identity, request)

        assertThat(first).isSuccess()
        assertThat(second).isEqualTo(first)

        assertThat(second).isSuccess().given {
            assertThat(it.publishedAt).isLessThan(Clock.System.now() - 1.seconds)
        }
    }

    @Test
    fun `should return inserted record`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val first = service.publish(topic, identity, request)

        assertThat(first).isSuccess().given {
            assertThat(it).all {
                prop(PublishResponse::topic).isEqualTo(topic.name)
                prop(PublishResponse::sequence).isEqualTo(1)
                prop(PublishResponse::recordKey).isEqualTo(request.recordKey)
                prop(PublishResponse::idempotencyKey).isEqualTo(request.idempotencyKey)
                prop(PublishResponse::correlationId).isEqualTo(request.correlationId)
                prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
            }
        }
    }

    @Test
    fun `should return failure for invalid topic names`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val invalidTopic = topic.copy(
            name = "invalid".repeat(10)
        )
        val result = service.publish(invalidTopic, identity, request)

        assertThat(result).isFailure()
    }

    @Test
    fun `should return failure for invalid record keys`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val invalidRequest = request.copy(
            recordKey = "1234567890".repeat(30)
        )
        val result = service.publish(topic, identity, invalidRequest)

        assertThat(result).isFailure()
    }

    @Test
    fun `high concurrency should not break sequencing`(): Unit = runBlocking {
        val workers = 10
        val recordsPerWorker = 100
        val numberOfTopics = 2
        val service = Records.ServiceImpl(dataSource())

        val timeToInsert = measureTime {
            coroutineScope {
                repeat(workers) { workerId ->
                    val topic = topic.copy(name = "topic-${workerId % numberOfTopics}")
                    launch(Dispatchers.IO) {
                        repeat(recordsPerWorker) { recordId ->
                            val r = PublishRequest(
                                recordKey = "${workerId}-${recordId}",
                                idempotencyKey = "${workerId}-${recordId}",
                                correlationId = Uuid.random(),
                                payload = "${workerId}-${recordId}".toByteArray(),
                            )
                            service.publish(topic, identity, r)
                        }
                    }
                }
            }
        }

        val heads = buildList {
            dataSource().withSession {
                repeat(numberOfTopics) { topicId ->
                    val topic = topic.copy(name = "topic-${topicId % numberOfTopics}")
                    add(currentHeadForTopic(topic))
                }
            }
        }

        assertThat(heads).isEqualTo(List(numberOfTopics) { (workers * recordsPerWorker).toLong() / numberOfTopics })
        assertThat(timeToInsert).isLessThan(5.seconds) // Around 1sec on OSX M1
    }
}