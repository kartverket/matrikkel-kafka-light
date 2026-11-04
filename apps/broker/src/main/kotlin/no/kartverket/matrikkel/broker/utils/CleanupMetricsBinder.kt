package no.kartverket.matrikkel.broker.utils

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.service.cleanup.Cleanup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Registers cleanup meters for topics with a cleanup policy. Cleanup results are fed in through [record]. */
class CleanupMetricsBinder(topics: Collection<Topic>) : MeterBinder {
    private val topicNames = topics.filter { it.cleanupPolicies.isNotEmpty() }.map { it.name }
    private val maxCompactableSequences = ConcurrentHashMap<String, AtomicLong>()
    private val counters = ConcurrentHashMap<Pair<String, String>, Counter>()

    override fun bindTo(registry: MeterRegistry) {
        topicNames.forEach { topic ->
            val value = maxCompactableSequences.computeIfAbsent(topic) { AtomicLong(0) }
            Gauge.builder("cleanup_max_compactable_sequence", value) { it.get().toDouble() }
                .tag("topic", topic)
                .register(registry)

            listOf("compact", "delete").forEach { policy ->
                counters[topic to policy] = registry.counter("records_deleted", "topic", topic, "policy", policy)
            }
        }
    }

    fun record(result: Cleanup.Result) {
        maxCompactableSequences[result.topic]?.set(result.maxCompactableSequence)
        counters[result.topic to "compact"]?.increment(result.deletedByCompact.toDouble())
        counters[result.topic to "delete"]?.increment(result.deletedByRetention.toDouble())
    }
}
