package no.kartverket.matrikkel.broker.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import javax.sql.DataSource

class TopicMetrics(
    val registry: MeterRegistry,
) {
    private val heads = ConcurrentHashMap<HeadKey, AtomicLong>()

    suspend fun initialize(topics: List<Topic>,
                           recordsRepository: RecordsRepository,
                           datasource: DataSource,) {
        topics.forEach { topic ->
            datasource.withSession {  }
            datasource.withSession { recordsRepository.currentHeadForTopic(topic) }
                .let { head ->
                    register(topic, head)
                }
        }
    }

    fun register(topic: Topic, head: Long): AtomicLong {
        val key = HeadKey(topic.name)

        val head = heads.computeIfAbsent(key) { AtomicLong(head) }

        Gauge.builder("topic.head", head) {
            it.toDouble()
        }
            .tag("topic", topic.name)
            .register(registry)

        return head
    }

    fun update(topic: Topic, newHead: Long) {
        val head = heads[HeadKey(topic.name)] ?: register(topic, newHead)
        head.set(newHead)
    }

    private data class HeadKey(val topicName: String)
}
