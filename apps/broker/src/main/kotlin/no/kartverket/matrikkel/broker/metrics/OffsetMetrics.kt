package no.kartverket.matrikkel.broker.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import javax.sql.DataSource

class OffsetMetrics(
    val registry: MeterRegistry,
) {
    private val offsets = ConcurrentHashMap<OffsetKey, AtomicLong>()

    suspend fun initialize(topics: List<Topic>,
                           offsetRepository: OffsetRepository,
                           datasource: DataSource,) {
        topics.forEach { topic ->
            datasource.withSession { offsetRepository.getOffsets(topic) }
                .forEach { (consumerGroup, offset) ->
                    register(topic, consumerGroup, offset)
                }
        }
    }

    fun register(topic: Topic, consumerGroup: String, offset: Long): AtomicLong {
        val key = OffsetKey(topic.name, consumerGroup)

        val offset = offsets.computeIfAbsent(key) { AtomicLong(offset) }

        Gauge.builder("consumer.offset", offset) {
            it.toDouble()
        }
            .tag("topic", topic.name)
            .tag("consumerGroup", consumerGroup)
            .register(registry)

        return offset
    }


    fun update(topic: Topic, consumerGroup: String, newOffset: Long) {
        val offset = offsets[OffsetKey(topic.name, consumerGroup)] ?: register(topic, consumerGroup, newOffset)
        offset.set(newOffset)

    }

    private data class OffsetKey(val topicName: String, val consumerGroup: String)
}
