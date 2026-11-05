package no.kartverket.matrikkel.broker.utils

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.service.cleanup.Cleanup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Registers cleanup meters per topic policy. Cleanup results are fed in through [record]. */
class CleanupMetricsBinder(private val topics: Collection<Topic>) : MeterBinder {
    private enum class Policy {
        COMPACT, DELETE;

        val tag = name.lowercase()
    }

    private val counters = ConcurrentHashMap<Pair<String, Policy>, Counter>()

    override fun bindTo(registry: MeterRegistry) {
        topics.forEach { topic ->
            if (topic.cleanupPolicies.any { it is CleanupPolicy.Compact }) {
                counters[topic.name to Policy.COMPACT] = deletedCounter(registry, topic, Policy.COMPACT)
            }
            if (topic.cleanupPolicies.any { it is CleanupPolicy.Delete }) {
                counters[topic.name to Policy.DELETE] = deletedCounter(registry, topic, Policy.DELETE)
            }
        }
    }

    private fun deletedCounter(registry: MeterRegistry, topic: Topic, policy: Policy) =
        registry.counter("records_deleted", "topic", topic.name, "policy", policy.tag)

    fun record(result: Cleanup.Result) {
        counters[result.topic to Policy.COMPACT]?.increment(result.deletedByCompact.toDouble())
        counters[result.topic to Policy.DELETE]?.increment(result.deletedByRetention.toDouble())
    }
}
