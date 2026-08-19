package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isLessThan
import assertk.assertions.isNotEmpty
import assertk.assertions.isSuccess
import assertk.assertions.messageContains
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
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.service.records.RecordsRepository.currentHeadForTopic
import no.kartverket.matrikkel.kafkaclient.CommitRequest
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import no.kartverket.matrikkel.kafkaclient.PollRequest
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import no.kartverket.matrikkel.kafkaclient.SeekRequest
import no.kartverket.no.kartverket.matrikkel.broker.service.records.TestUtils.createLease
import no.kartverket.no.kartverket.matrikkel.broker.service.records.TestUtils.releaseLease
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
    val consumerGroup = "dummy-group"
    val identity = ServiceIdentity("fake-user")
    val publishRequest = PublishRequest(
        idempotencyKey = "idempotency_key",
        records = listOf(
            PublishRecord(
                key = "record_key".toByteArray(),
                payload = "custom payload".toByteArray()
            )
        ),
    )
    val pollRequest = PollRequest(
        maxRecords = 10,
        consumerGroup = consumerGroup,
        instanceId = "dummy-instanceId",
        initialOffsetPolicy = InitialOffsetPolicy.EARLIEST
    )

    @Test
    fun `should return existing published record if found`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val first = service.publish(ctx, publishRequest)

        delay(1.seconds)

        val second = service.publish(ctx, publishRequest)

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
        val first = service.publish(ctx, publishRequest)

        assertThat(first).isSuccess().given {
            assertThat(it).all {
                prop(PublishResponse::topic).isEqualTo(topic.name)
                prop(PublishResponse::sequence).isEqualTo(1)
                prop(PublishResponse::idempotencyKey).isEqualTo(publishRequest.idempotencyKey)
                prop(PublishResponse::publishedAt).isApproxNow(1.seconds)
            }
        }
    }

    @Test
    fun `should return failure for invalid record keys`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val invalidRequest = publishRequest.copy(
            records = publishRequest.records.map { it.copy(key = "1234567890".repeat(30).toByteArray()) }
        )
        val result = service.publish(ctx, invalidRequest)

        assertThat(result).isFailure()
    }

    @Test
    fun `should return Pollresponse with lease and empty list of records`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        val pollResponseResult = service.poll(ctx, pollRequest)


        assertThat(pollResponseResult).isSuccess()
            .given {
                assertThat(it.leaseToken).isNotEmpty()
                assertThat(it.records).isEmpty()
            }
    }

    @Test
    fun `should return Pollresponse with lease and list of records`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())
        service.publish(ctx, publishRequest)

        val pollResponseResult = service.poll(ctx, pollRequest)


        assertThat(pollResponseResult).isSuccess()
            .given {
                assertThat(it.leaseToken).isNotEmpty()
                assertThat(it.records).hasSize(1)
                assertThat(it.records.first().key.contentToString())
                    .isEqualTo(publishRequest.records.first().key.contentToString())
                assertThat(it.records.first().payload?.contentToString())
                    .isEqualTo(publishRequest.records.first().payload?.contentToString())
            }
    }

    @Test
    fun `commit should fail if no offset is stored (its created on initiall poll)`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        val lease = createLease(topic, consumerGroup)
        val result = service.commit(ctx, CommitRequest(lease.token, 123L))

        assertThat(result).isFailure()
            .messageContains("Cannot commit when offset does not exist")
    }

    @Test
    fun `commit should fail if offset is lower than the current offset`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        val poll = service.poll(ctx, pollRequest).getOrThrow()

        val result = service.commit(ctx, CommitRequest(poll.leaseToken, -123L))

        assertThat(result).isFailure()
            .messageContains("Sequence must be larger then current offset")
    }

    @Test
    fun `commit should fail if offset is higher than the topic head`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        val poll = service.poll(ctx, pollRequest).getOrThrow()

        val result = service.commit(ctx, CommitRequest(poll.leaseToken, 123L))

        assertThat(result).isFailure()
            .messageContains("Sequence must not be greater than the topic head")
    }

    @Test
    fun `commit update offset and return an updated leasetoken`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        service.publish(ctx, publishRequest)
        val poll = service.poll(ctx, pollRequest).getOrThrow()

        val result = service.commit(ctx, CommitRequest(poll.leaseToken, poll.records.last().sequence))

        assertThat(result).isSuccess()
    }

    @Test
    fun `seek should fail if seeking below 0`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        val result = service.seek(ctx, SeekRequest(consumerGroup, -1L))

        assertThat(result).isFailure()
            .messageContains("Cannot seek to any offset lower than 0")
    }

    @Test
    fun `seek should fail if seeking ahead of topichead`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        val result = service.seek(ctx, SeekRequest(consumerGroup, 10L))

        assertThat(result).isFailure()
            .messageContains("Sequence must not be greater than the topic head")
    }

    @Test
    fun `seek should fail a active lease is present for the consumergroup`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        service.poll(ctx, pollRequest)
        val result = service.seek(ctx, SeekRequest(consumerGroup, 0L))

        assertThat(result).isFailure()
            .messageContains("A current lease prevents seeking for this consumer group")
    }

    @Test
    fun `seek should update the offset`(): Unit = runBlocking {
        val service = Records.ServiceImpl(dataSource())
        val ctx = Records.Service.Ctx(topic, identity, Uuid.random())

        service.publish(ctx, publishRequest)
        val poll = service.poll(ctx, pollRequest).getOrThrow()
        service.commit(ctx, CommitRequest(poll.leaseToken, 1L))
        val initialOffset = dataSource().withTransaction {
            withLease(topic, poll.leaseToken) {
                OffsetRepository.getOffsetOrNull(topic, consumerGroup)
            }
        }
        assertThat(initialOffset).isSuccess().isEqualTo(1)
        releaseLease(topic)

        val result = service.seek(ctx, SeekRequest(consumerGroup, 0L))

        assertThat(result).isSuccess()
        val offset = dataSource().withTransaction {
            withLease(topic, consumerGroup, "test-instance") {
                OffsetRepository.getOffsetOrNull(topic, consumerGroup)
            }
        }
        assertThat(offset).isSuccess().isEqualTo(0)
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