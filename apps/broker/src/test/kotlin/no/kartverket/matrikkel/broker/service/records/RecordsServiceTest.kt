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
import no.kartverket.matrikkel.kafkaclient.PublishRecord
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
        idempotencyKey = "idempotency_key",
        records = listOf(
            PublishRecord(
                key = "record_key".toByteArray(),
                payload = "custom payload".toByteArray()
            )
        ),
    )

    @Test
    fun `should return existing published record if found`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val first = service.publish(ctx, request)

        delay(1.seconds)

        val second = service.publish(ctx, request)

        assertThat(first).isSuccess()
        assertThat(second).isEqualTo(first)

        assertThat(second).isSuccess().given {
            assertThat(it.publishedAt).isLessThan(Clock.System.now() - 1.seconds)
        }
    }

    @Test
    fun `should return inserted record`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val first = service.publish(ctx, request)

        assertThat(first).isSuccess().given {
            assertThat(it).all {
                prop(PublishResponse::topic).isEqualTo(topic.name)
                prop(PublishResponse::sequence).isEqualTo(1)
                prop(PublishResponse::idempotencyKey).isEqualTo(request.idempotencyKey)
                prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
            }
        }
    }

    @Test
    fun `should return failure for invalid record keys`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val invalidRequest = request.copy(
            records = request.records.map { it.copy(key = "1234567890".repeat(30).toByteArray()) }
        )
        val result = service.publish(ctx, invalidRequest)

        assertThat(result).isFailure()
    }

    @Test
    fun `high concurrency should not break sequencing`(): Unit = runBlocking {
        val workers = 10
        val recordsPerWorker = 400
        val numberOfTopics = 2
        val numberOfRecordsPerPublish = 4
        val service = Records.ServiceImpl(dataSource())

        val timeToInsert = measureTime {
            coroutineScope {
                repeat(workers) { workerId ->
                    val topic = topic.copy(name = "topic-${workerId % numberOfTopics}")
                    launch(Dispatchers.IO) {
                        val batches = recordsPerWorker / numberOfRecordsPerPublish
                        var recordId = 0
                        repeat(batches) { batchId ->
                            val r = PublishRequest(
                                idempotencyKey = "$workerId-$batchId-$recordId",
                                records = buildList {
                                    repeat(numberOfRecordsPerPublish) {
                                        add(
                                            PublishRecord(
                                                key = "${workerId}-${recordId}".toByteArray(),
                                                payload = "${workerId}-${recordId}".toByteArray(),
                                            )
                                        )
                                        recordId++
                                    }
                                }
                            )
                            val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
                            service.publish(ctx, r)
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