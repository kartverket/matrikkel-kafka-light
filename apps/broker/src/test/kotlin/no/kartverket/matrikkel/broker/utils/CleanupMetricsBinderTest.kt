package no.kartverket.matrikkel.broker.utils

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.service.cleanup.Cleanup
import org.junit.jupiter.api.Test

class CleanupMetricsBinderTest {
    private val acl = TopicAccessControlList(setOf("*"), setOf("*"))
    private val compacted = Topic("compacted", acl, cleanupPolicies = setOf(CleanupPolicy.Compact()))
    private val plain = Topic("plain", acl)

    private val registry = SimpleMeterRegistry()
    private val binder = CleanupMetricsBinder(listOf(compacted, plain)).also { it.bindTo(registry) }

    private fun count(topic: String, policy: String) =
        registry.get("records_deleted").tags("topic", topic, "policy", policy).counter().count()

    @Test
    fun `records counters per policy and max cleanable sequence gauge`() {
        binder.record(Cleanup.Result("compacted", deletedByCompact = 1, deletedByRetention = 2, maxCleanableSequence = 4))
        binder.record(Cleanup.Result("compacted", deletedByCompact = 0, deletedByRetention = 0, maxCleanableSequence = 7))

        assertThat(count("compacted", "compact")).isEqualTo(1.0)
        assertThat(count("compacted", "delete")).isEqualTo(2.0)
        assertThat(registry.get("cleanup_max_cleanable_sequence").tag("topic", "compacted").gauge().value())
            .isEqualTo(7.0)
    }

    @Test
    fun `registers meters only for topics with a cleanup policy`() {
        assertThat(registry.find("cleanup_max_cleanable_sequence").tag("topic", "plain").gauge()).isNull()
    }
}
