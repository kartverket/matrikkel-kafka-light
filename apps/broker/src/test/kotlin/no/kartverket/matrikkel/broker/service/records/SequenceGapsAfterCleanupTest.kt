package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isSuccess
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.service.cleanup.Cleanup
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.kafkaclient.CommitRequest
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import no.kartverket.matrikkel.kafkaclient.PollRequest
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.no.kartverket.matrikkel.broker.testutils.RawRecord
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.insertRawRecords
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import kotlin.uuid.Uuid

class SequenceGapsAfterCleanupTest : WithDatabase {
    private val topic = Topic(
        name = "gap-topic",
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        cleanupPolicies = setOf(CleanupPolicy.Compact()),
    )
    private val ctx = Records.Service.Ctx(topic, ServiceIdentity("fake-user"), Uuid.random())
    private val pollRequest = PollRequest(
        maxRecords = 10,
        consumerGroup = "gap-group",
        instanceId = "gap-instance",
        initialOffsetPolicy = InitialOffsetPolicy.EARLIEST,
    )

    /** Sequences 1..5 (keys A,B,A,B,A), all old. After cleanup only 4 and 5 survive. */
    private fun seedAndCleanup(): Records.Service {
        val old = Clock.System.now() - 10.days
        dataSource().insertRawRecords(
            topic.name,
            listOf("A", "B", "A", "B", "A").mapIndexed { i, key -> RawRecord(i + 1L, key, old) }
        )
        Cleanup.ServiceImpl(dataSource()).cleanup(topic)
        return Records.ServiceImpl(dataSource())
    }

    @Test
    fun `poll skips removed sequences and returns survivors in order`(): Unit = runBlocking {
        val service = seedAndCleanup()

        val poll = service.poll(ctx, pollRequest).getOrThrow()

        assertThat(poll.records.map { it.sequence }).isEqualTo(listOf(4L, 5L))
    }

    @Test
    fun `commit at a removed sequence succeeds but above head still fails`(): Unit = runBlocking {
        val service = seedAndCleanup()
        val poll = service.poll(ctx, pollRequest).getOrThrow()

        assertThat(service.commit(ctx, CommitRequest(poll.leaseToken, 2L))).isSuccess()
        assertThat(service.commit(ctx, CommitRequest(poll.leaseToken, 6L))).isFailure()
    }

    @Test
    fun `publish after cleanup continues from head without reusing sequences`(): Unit = runBlocking {
        val service = seedAndCleanup()

        val response = service.publish(
            ctx,
            PublishRequest("after-cleanup", listOf(PublishRecord("A".toByteArray(), "new".toByteArray())))
        ).getOrThrow()

        assertThat(response.sequence).isEqualTo(6L)
        val poll = service.poll(ctx, pollRequest).getOrThrow()
        assertThat(poll.records.map { it.sequence }).isEqualTo(listOf(4L, 5L, 6L))
    }
}
