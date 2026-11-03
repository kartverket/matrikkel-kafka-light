package no.kartverket.matrikkel.broker.service.cleanup

import assertk.assertThat
import assertk.assertions.isEqualTo
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.no.kartverket.matrikkel.broker.testutils.*
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Instant

class CleanupServiceTest : WithDatabase {
    private val now = Instant.parse("2026-01-20T12:00:00Z")
    private val clock = FakeClock(now)

    private fun topic(
        policies: Set<CleanupPolicy>,
        name: String = "cleanup-topic",
        minCleanableAge: Duration = 1.days,
    ) = Topic(
        name = name,
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        cleanupPolicies = policies,
        minCleanableAge = minCleanableAge,
    )

    private fun service(batchSize: Int = 1000): Cleanup.Service =
        Cleanup.ServiceImpl(dataSource(), batchSize = batchSize, clock = clock)

    private fun rawRecord(
        seq: Long,
        key: String,
        age: Duration,
        payload: String? = "v",
        producer: String = "producer-a"
    ) =
        RawRecord(seq, key, now - age, payload, producer)

    private fun seed(topic: Topic, vararg rows: RawRecord) = dataSource().insertRawRecords(topic.name, rows.toList())

    private fun remaining(topic: Topic) = dataSource().storedSequences(topic.name)

    inner class TopicBuilder(val records: MutableList<RawRecord> = mutableListOf()) {
        var seq: Long = 1L
        fun record(
            key: String,
            age: Duration,
            payload: String? = "v",
            producer: String = "producer-a"
        ) {
            records += RawRecord(seq++, key, now - age, payload, producer)
        }
    }

    private fun topicMock(
        name: String = "cleanup-topic",
        minCleanableAge: Duration = 1.days,
        policies: Set<CleanupPolicy> = setOf(),
        fn: TopicBuilder.() -> Unit,
    ): Topic {
        val topic = topic(name = name, minCleanableAge = minCleanableAge, policies = policies)
        val builder = TopicBuilder().apply(fn)
        seed(topic, *builder.records.toTypedArray())
        return topic
    }

    @Test
    fun `empty policy set deletes nothing`() {
        val topic = topicMock {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
            record(key = "A", age = 8.days, payload = null)
        }

        val result = service().cleanup(topic)

        assertThat(result.totalDeleted).isEqualTo(0)
        assertThat(remaining(topic)).isEqualTo(listOf(1L, 2L, 3L))
    }

    @Test
    fun `compact removes older versions but keeps latest per key`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 10.days)
            record(key = "A", age = 9.days)
            record(key = "B", age = 9.days)
            record(key = "A", age = 8.days)
        }

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(4L, 5L))
        assertThat(result.topic).isEqualTo(topic.name)
        assertThat(result.deletedByCompact).isEqualTo(3)
        assertThat(result.deletedByRetention).isEqualTo(0)
        assertThat(result.maxCleanableSequence).isEqualTo(4L)
    }

    @Test
    fun `records younger than minCleanableAge are kept even when superseded`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 2.hours)
            record(key = "A", age = 1.hours)
        }

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L, 3L))
        assertThat(result.maxCleanableSequence).isEqualTo(1L)
    }

    @Test
    fun `head is never deleted even when it is an old tombstone`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact(tombstoneRetention = 1.hours))) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 10.days)
        }

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(result.maxCleanableSequence).isEqualTo(1L)
    }

    @Test
    fun `head is never deleted by delete policy even when older than retention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 2.days))) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 10.days)
        }
        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
    }

    @Test
    fun `tombstone is removed only after tombstoneRetention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact(tombstoneRetention = 5.days))) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 10.days, payload = null)
            record(key = "B", age = 3.days, payload = null)
            record(key = "C", age = 2.days)
        }

        service().cleanup(topic)

        // A: value superseded and old tombstone removed. B: tombstone younger than retention is kept.
        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L))
    }

    @Test
    fun `tombstone above max cleanable sequence is kept even when older than tombstoneRetention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact(tombstoneRetention = 1.hours))) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 2.hours, payload = null)
            record(key = "C", age = 1.hours)
        }

        val result = service().cleanup(topic)

        assertThat(result.maxCleanableSequence).isEqualTo(1L)
        assertThat(remaining(topic)).isEqualTo(listOf(1L, 2L, 3L))
    }

    @Test
    fun `delete policy removes records older than retention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 3.days))) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 5.days)
            record(key = "C", age = 2.days)
            record(key = "D", age = 12.hours)
            record(key = "E", age = 1.hours)
        }

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByRetention).isEqualTo(2)
        assertThat(result.deletedByCompact).isEqualTo(0)
        assertThat(result.maxCleanableSequence).isEqualTo(3L)
    }

    @Test
    fun `compact and delete combined count deletions per policy`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 7.days), CleanupPolicy.Compact())) {
            record(key = "A", age = 6.days)
            record(key = "B", age = 8.days)
            record(key = "A", age = 5.days)
            record(key = "C", age = 2.days)
            record(key = "D", age = 1.hours)
        }

        val result = service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByCompact).isEqualTo(1)
        assertThat(result.deletedByRetention).isEqualTo(1)
        assertThat(result.totalDeleted).isEqualTo(2)
    }

    @Test
    fun `small batch size still cleans everything`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            repeat(25) { record(key = "A", age = 10.days) }
            record(key = "A", age = 1.hours)
        }

        val result = service(batchSize = 2).cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(26L))
        assertThat(result.deletedByCompact).isEqualTo(25)
    }

    @Test
    fun `same key from different producers is compacted`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days, producer = "producer-a")
            record(key = "A", age = 9.days, producer = "producer-b")
            record(key = "A", age = 8.days, producer = "producer-c")
        }

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L))
    }

    @Test
    fun `other topics are untouched`() {

        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 2.days), CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
        }

        val other = topicMock(name = "other-topic", policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
        }

        service().cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(remaining(other)).isEqualTo(listOf(1L, 2L))
    }
}
