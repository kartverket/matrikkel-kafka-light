package no.kartverket.matrikkel.broker.service.cleanup

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.no.kartverket.matrikkel.broker.testutils.RawRecord
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.insertRawRecords
import no.kartverket.no.kartverket.matrikkel.broker.testutils.storedSequences
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Instant

class CleanupServiceTest : WithDatabase {
    private val now = Instant.parse("2026-01-20T12:00:00Z")
    private val clock = object : Clock {
        override fun now() = now
    }

    private val service: Cleanup.Service = Cleanup.ServiceImpl(dataSource(), batchSize = 2, clock = clock)

    private fun cleanup(topic: Topic) = runBlocking { service.cleanup(topic) }

    private fun remaining(topic: Topic) = dataSource().storedSequences(topic.name)

    private inner class RecordsBuilder {
        val records = mutableListOf<RawRecord>()

        fun record(
            key: String,
            age: Duration,
            payload: String? = "v",
            producer: String = "producer-a"
        ) {
            records += RawRecord(records.size + 1L, key, now - age, payload, producer)
        }
    }

    private fun topicMock(
        name: String = "cleanup-topic",
        policies: Set<CleanupPolicy> = emptySet(),
        build: RecordsBuilder.() -> Unit,
    ): Topic {
        val topic = Topic(
            name = name,
            acl = TopicAccessControlList(setOf("*"), setOf("*")),
            cleanupPolicies = policies,
        )
        dataSource().insertRawRecords(topic.name, RecordsBuilder().apply(build).records)
        return topic
    }

    @Test
    fun `empty policy set deletes nothing`() {
        val topic = topicMock {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
            record(key = "A", age = 8.days, payload = null)
        }

        val result = cleanup(topic)

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

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(4L, 5L))
        assertThat(result.deletedByCompact).isEqualTo(3)
        assertThat(result.deletedByRetention).isEqualTo(0)
        assertThat(result.maxCompactableSequence).isEqualTo(4L)
    }

    @Test
    fun `records younger than minCompactionLag are kept even when superseded`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 2.hours)
            record(key = "A", age = 1.hours)
        }

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L, 3L))
        assertThat(result.maxCompactableSequence).isEqualTo(1L)
    }

    @Test
    fun `head is never deleted even when it is an old tombstone`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact(tombstoneRetention = 1.hours))) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 10.days, payload = null)
        }

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(result.maxCompactableSequence).isEqualTo(1L)
    }

    @Test
    fun `head is never deleted by delete policy even when older than retention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 2.days))) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 10.days)
        }

        cleanup(topic)

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

        cleanup(topic)

        // A: value superseded and old tombstone removed. B: tombstone younger than retention is kept.
        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L))
    }

    @Test
    fun `tombstone above maxCompactableSequence is kept even when older than tombstoneRetention`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact(tombstoneRetention = 1.hours))) {
            record(key = "A", age = 10.days)
            record(key = "B", age = 2.hours, payload = null)
            record(key = "C", age = 1.hours)
        }

        val result = cleanup(topic)

        assertThat(result.maxCompactableSequence).isEqualTo(1L)
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

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByRetention).isEqualTo(2)
        assertThat(result.deletedByCompact).isEqualTo(0)
        assertThat(result.maxCompactableSequence).isEqualTo(0L)
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

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L, 4L, 5L))
        assertThat(result.deletedByCompact).isEqualTo(1)
        assertThat(result.deletedByRetention).isEqualTo(1)
        assertThat(result.totalDeleted).isEqualTo(2)
    }

    @Test
    fun `deletes more records than fit in one batch`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            repeat(5) { record(key = "A", age = 10.days) }
            record(key = "A", age = 1.hours)
        }

        val result = cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(6L))
        assertThat(result.deletedByCompact).isEqualTo(5)
    }

    @Test
    fun `same key from different producers is compacted`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days, producer = "producer-a")
            record(key = "A", age = 9.days, producer = "producer-b")
            record(key = "A", age = 8.days, producer = "producer-c")
        }

        cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(3L))
    }

    @Test
    fun `other topics are untouched`() {
        val topic = topicMock(policies = setOf(CleanupPolicy.Delete(retention = 2.days), CleanupPolicy.Compact())) {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
        }
        val other = topicMock(policies = setOf(CleanupPolicy.Compact()), name = "other-topic") {
            record(key = "A", age = 10.days)
            record(key = "A", age = 9.days)
        }

        cleanup(topic)

        assertThat(remaining(topic)).isEqualTo(listOf(2L))
        assertThat(remaining(other)).isEqualTo(listOf(1L, 2L))
    }
}
