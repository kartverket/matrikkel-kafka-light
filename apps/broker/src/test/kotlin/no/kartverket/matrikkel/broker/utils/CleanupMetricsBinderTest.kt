package no.kartverket.matrikkel.broker.utils

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.service.cleanup.Cleanup
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.days

class CleanupMetricsBinderTest {
    private val acl = TopicAccessControlList(setOf("*"), setOf("*"))
    private val both = Topic(
        "both", acl, compactionPolicy = CleanupPolicy.Compact(), deletionPolicy =  CleanupPolicy.Delete(30.days))

    private val deleteOnly = Topic("delete-only", acl, deletionPolicy = CleanupPolicy.Delete(30.days))
    private val plain = Topic("plain", acl)

    private val registry = SimpleMeterRegistry()
    private val binder = CleanupMetricsBinder(listOf(both, deleteOnly, plain)).also { it.bindTo(registry) }

    private fun count(topic: String, policy: String) =
        registry.get("records_deleted").tags("topic", topic, "policy", policy).counter().count()

    private fun hasCounter(topic: String, policy: String) =
        registry.find("records_deleted").tags("topic", topic, "policy", policy).counter() != null

    @Test
    fun `records counters per policy`() {
        binder.record(Cleanup.Result("both", deletedByCompact = 1, deletedByRetention = 2))
        binder.record(Cleanup.Result("both", deletedByCompact = 0, deletedByRetention = 0))

        assertThat(count("both", "compact")).isEqualTo(1.0)
        assertThat(count("both", "delete")).isEqualTo(2.0)
    }

    @Test
    fun `registers only meters matching the topic policies`() {
        assertThat(hasCounter("both", "compact")).isTrue()
        assertThat(hasCounter("both", "delete")).isTrue()

        assertThat(hasCounter("delete-only", "compact")).isFalse()
        assertThat(hasCounter("delete-only", "delete")).isTrue()

        assertThat(hasCounter("plain", "delete")).isFalse()
    }
}
