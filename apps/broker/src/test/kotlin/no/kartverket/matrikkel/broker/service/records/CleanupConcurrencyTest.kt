package no.kartverket.no.kartverket.matrikkel.broker.service.records

import assertk.assertThat
import assertk.assertions.containsAll
import assertk.assertions.containsAtLeast
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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
import no.kartverket.no.kartverket.matrikkel.broker.testutils.storedSequences
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class CleanupConcurrencyTest : WithDatabase {
    private val topic = Topic(
        name = "concurrent-cleanup",
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        compactionPolicy = CleanupPolicy.Compact(),
    )
    private val ctx = Records.Service.Ctx(topic, ServiceIdentity("fake-user"), Uuid.random())
    private val oldRows = 4000
    private val distinctKeys = 40

    /** [oldRows] old records over [distinctKeys] keys; only the last [distinctKeys] rows are the latest per key. */
    private fun seedOldRecords() {
        val old = Clock.System.now() - 10.days
        dataSource().insertRawRecords(
            topic.name,
            (1..oldRows).map { RawRecord(it.toLong(), "k${it % distinctKeys}", old) }
        )
    }

    private val expectedOldSurvivors = ((oldRows - distinctKeys + 1)..oldRows).map { it.toLong() }

    @Test
    fun `publish and poll keep working while cleanup runs`(): Unit = runBlocking {
        seedOldRecords()
        val records = Records.ServiceImpl(dataSource())
        val cleanup = Cleanup.ServiceImpl(dataSource(), batchSize = 20)
        val published = mutableListOf<Long>()
        val polled = mutableListOf<Long>()
        val pollRequest = PollRequest(
            maxRecords = 200,
            consumerGroup = "cc-group",
            instanceId = "cc-instance",
            initialOffsetPolicy = InitialOffsetPolicy.EARLIEST,
        )

        withTimeout(60.seconds) {
            val cleaning = launch {
                while (published.isEmpty()) delay(5.milliseconds)
                repeat(3) { cleanup.cleanup(topic) }
            }
            launch {
                var i = 0
                while (cleaning.isActive) {
                    val request = PublishRequest(
                        "idem-$i",
                        listOf(PublishRecord("p$i".toByteArray(), "payload-$i".toByteArray()))
                    )
                    published += records.publish(ctx, request).getOrThrow().sequence
                    i++
                }
            }
            launch {
                while (cleaning.isActive) {
                    val poll = records.poll(ctx, pollRequest).getOrThrow()
                    if (poll.records.isNotEmpty()) {
                        polled += poll.records.map { it.sequence }
                        records.commit(ctx, CommitRequest(poll.leaseToken, poll.records.last().sequence))
                            .getOrThrow()
                    }
                }
            }
        }

        assertThat(published.size).isGreaterThan(1)
        assertThat(published).isEqualTo(published.sorted().distinct())
        assertThat(published.first()).isGreaterThan(oldRows.toLong())
        assertThat(polled).isEqualTo(polled.sorted().distinct())

        val stored = dataSource().storedSequences(topic.name)
        assertThat(stored.filter { it <= oldRows.toLong() }).isEqualTo(expectedOldSurvivors)
        assertThat(stored).containsAtLeast(*published.toTypedArray())
        assertThat(stored.last()).isEqualTo(published.last())
    }

    @Test
    fun `two concurrent cleanups of the same topic end in a consistent state`(): Unit = runBlocking {
        seedOldRecords()
        val cleanup = Cleanup.ServiceImpl(dataSource(), batchSize = 20)

        val results = withTimeout(60.seconds) {
            (1..2).map { async { cleanup.cleanup(topic) } }.awaitAll()
        }

        assertThat(results.sumOf { it.totalDeleted }).isEqualTo(oldRows - distinctKeys)
        assertThat(dataSource().storedSequences(topic.name)).isEqualTo(expectedOldSurvivors)
    }
}
