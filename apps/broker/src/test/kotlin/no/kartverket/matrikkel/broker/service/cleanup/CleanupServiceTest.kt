package no.kartverket.matrikkel.broker.service.cleanup

import assertk.assertThat
import assertk.assertions.isEqualTo
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.no.kartverket.matrikkel.broker.testutils.FakeClock
import no.kartverket.no.kartverket.matrikkel.broker.testutils.RawRecord
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.insertRawRecords
import no.kartverket.no.kartverket.matrikkel.broker.testutils.storedSequences
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Instant

class CleanupServiceTest : WithDatabase {
    private val now = Instant.parse("2026-01-20T12:00:00Z")
    private val clock = FakeClock(now)

    private fun topic(
        vararg policies: CleanupPolicy,
        name: String = "cleanup-topic",
        minCleanableAge: Duration = 1.days,
    ) = Topic(
        name = name,
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        cleanupPolicies = policies.toSet(),
        minCleanableAge = minCleanableAge,
    )

    private fun service(batchSize: Int = 1000) =
        Cleanup.ServiceImpl(dataSource(), batchSize = batchSize, clock = clock)

    private fun row(seq: Long, key: String, age: Duration, payload: String? = "v", producer: String = "producer-a") =
        RawRecord(seq, key, now - age, payload, producer)

    private fun seed(topic: Topic, vararg rows: RawRecord) = dataSource().insertRawRecords(topic.name, rows.toList())

    private fun remaining(topic: Topic) = dataSource().storedSequences(topic.name)

    @Test
    fun `empty policy set deletes nothing`() {
        val topic = topic()
        seed(topic, row(1, "A", 10.days), row(2, "A", 9.days), row(3, "A", 8.days, payload = null))

        val result = service().cleanup(topic)

        assertThat(result.totalDeleted).isEqualTo(0)
        assertThat(remaining(topic)).isEqualTo(listOf(1L, 2L, 3L))
    }

    @Test
    fun `compact removes older versions but keeps latest per key`() {
        val topic = topic(CleanupPolicy.Compact())
        seed(
            topic,
            row(1, "A", 10.days), row(2, "B", 10.days), row(3, "A", 9.days),
            row(4, "B", 9.days), row(5, "A", 8.days),
        )

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(4L, 5L))
        assertThat(result.topic).isEqualTo(topic.name)
        assertThat(result.deletedByCompact).isEqualTo(3)
        assertThat(result.deletedByRetention).isEqualTo(0)
        assertThat(result.maxCleanableSequence).isEqualTo(4L)
    }

    @Test
    fun `records younger than minCleanableAge are kept even when superseded`() {
        val topic = topic(CleanupPolicy.Compact())
        seed(topic, row(1, "A", 10.days), row(2, "A", 2.hours), row(3, "A", 1.hours))

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L, 3L))
        assertThat(result.maxCleanableSequence).isEqualTo(1L)
    }

    @Test
    fun `old superseded record is deleted when its newer version is young`() {
        val topic = topic(CleanupPolicy.Compact())
        seed(topic, row(1, "A", 10.days), row(2, "A", 1.hours))

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
    }

    @Test
    fun `head is never deleted even when it is an old tombstone`() {
        val topic = topic(CleanupPolicy.Compact(tombstoneRetention = 1.hours))
        seed(topic, row(1, "A", 10.days), row(2, "A", 10.days, payload = null))

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(result.maxCleanableSequence).isEqualTo(1L)
    }

    @Test
    fun `head is never deleted by delete policy even when older than retention`() {
        val topic = topic(CleanupPolicy.Delete(retention = 2.days))
        seed(topic, row(1, "A", 10.days), row(2, "B", 10.days))

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
    }

    @Test
    fun `tombstone is removed only after tombstoneRetention`() {
        val topic = topic(CleanupPolicy.Compact(tombstoneRetention = 5.days))
        seed(
            topic,
            row(1, "A", 10.days), row(2, "A", 10.days, payload = null),
            row(3, "C", 3.days, payload = null),
            row(4, "D", 2.days),
        )

        service().cleanup(topic)

        // A: value superseded and old tombstone removed. C: tombstone younger than retention is kept.
        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L))
    }

    @Test
    fun `tombstone above max cleanable sequence is kept even when older than tombstoneRetention`() {
        val topic = topic(CleanupPolicy.Compact(tombstoneRetention = 1.hours))
        seed(topic, row(1, "A", 10.days), row(2, "B", 2.hours, payload = null), row(3, "C", 1.hours))

        val result = service().cleanup(topic)

        assertThat(result.maxCleanableSequence).isEqualTo(1L)
        assertThat(remaining(topic)).isEqualTo(listOf(1L, 2L, 3L))
    }

    @Test
    fun `delete policy removes records older than retention`() {
        val topic = topic(CleanupPolicy.Delete(retention = 3.days))
        seed(
            topic,
            row(1, "A", 10.days), row(2, "B", 5.days), row(3, "C", 2.days),
            row(4, "D", 12.hours), row(5, "E", 1.hours),
        )

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByRetention).isEqualTo(2)
        assertThat(result.deletedByCompact).isEqualTo(0)
        assertThat(result.maxCleanableSequence).isEqualTo(3L)
    }

    @Test
    fun `compact and delete combined count deletions per policy`() {
        val topic = topic(CleanupPolicy.Compact(), CleanupPolicy.Delete(retention = 7.days))
        seed(
            topic,
            row(1, "A", 6.days), // superseded by 3 -> compact
            row(2, "B", 8.days), // latest for B but older than retention -> delete
            row(3, "A", 5.days), // latest for A, young enough -> kept
            row(4, "C", 2.days),
            row(5, "D", 1.hours),
        )

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByCompact).isEqualTo(1)
        assertThat(result.deletedByRetention).isEqualTo(1)
        assertThat(result.totalDeleted).isEqualTo(2)
    }

    @Test
    fun `small batch size still cleans everything`() {
        val topic = topic(CleanupPolicy.Compact())
        seed(topic, *Array(25) { row(it + 1L, "A", 10.days) }, row(26, "A", 1.hours))

        val result = service(batchSize = 2).cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(26L))
        assertThat(result.deletedByCompact).isEqualTo(25)
    }

    @Test
    fun `same key from different producers is compacted`() {
        val topic = topic(CleanupPolicy.Compact())
        seed(
            topic,
            row(1, "A", 10.days, producer = "producer-a"),
            row(2, "A", 9.days, producer = "producer-b"),
            row(3, "A", 8.days, producer = "producer-c"),
        )

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L))
    }

    @Test
    fun `other topics are untouched`() {
        val topic = topic(CleanupPolicy.Compact(), CleanupPolicy.Delete(retention = 2.days))
        val other = topic(CleanupPolicy.Compact(), name = "other-topic")
        seed(topic, row(1, "A", 10.days), row(2, "A", 9.days))
        seed(other, row(1, "A", 10.days), row(2, "A", 9.days))

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(remaining(other)).isEqualTo(listOf(1L, 2L))
    }
}
