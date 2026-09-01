package no.kartverket.matrikkel.broker.utils

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.MultiCollector
import io.prometheus.metrics.model.snapshots.GaugeSnapshot
import io.prometheus.metrics.model.snapshots.Labels
import io.prometheus.metrics.model.snapshots.MetricSnapshots
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.repository.withSession
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import javax.sql.DataSource
import kotlin.collections.iterator
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class TopicMetricsBinder(
    val topicCatalog: TopicCatalog,
    val dataSource: DataSource,
) : MeterBinder {
    override fun bindTo(registry: MeterRegistry) {
        require(registry is PrometheusMeterRegistry) {
            "TopicHeadMeterBinder requires PrometheusMeterRegistry"
        }

        val metrics: Map<Topic, TopicMetrics> by cache(10.seconds.toJavaDuration()) {
            getMetricsData(topicCatalog)
        }

        registry.prometheusRegistry.register(
            MultiCollector {
                val headSnapshot = GaugeSnapshot.builder()
                    .name("topic_head")
                    .help("Current head sequence for each topic")

                val offsetSnapshot = GaugeSnapshot.builder()
                    .name("consumer_offset")
                    .help("Offset for consumer")

                for ((topic, data) in metrics) {
                    headSnapshot.dataPoint(
                        GaugeSnapshot.GaugeDataPointSnapshot.builder()
                            .labels(Labels.of("topic", topic.name))
                            .value(data.head.toDouble())
                            .build()
                    )

                    for ((consumerGroup, offset) in data.offsets) {
                        offsetSnapshot.dataPoint(
                            GaugeSnapshot.GaugeDataPointSnapshot.builder()
                                .labels(
                                    Labels.of(
                                        "topic", topic.name,
                                        "consumer_group", consumerGroup,
                                    )
                                )
                                .value(offset.toDouble())
                                .build()
                        )
                    }
                }

                MetricSnapshots.of(
                    headSnapshot.build(),
                    offsetSnapshot.build(),
                )
            }
        )
    }


    private data class TopicMetrics(
        val head: Long,
        val offsets: Map<String, Long>,
    )

    private fun getMetricsData(topicCatalog: TopicCatalog): Map<Topic, TopicMetrics> {
        return runBlocking {
            dataSource.withSession {
                val heads: Map<Topic, Long> = topicCatalog.topics
                    .associateWith { RecordsRepository.currentHeadForTopic(it) }
                val offsets: Map<Topic, Map<String, Long>> = OffsetRepository.getOffsets(topicCatalog)

                topicCatalog.topics.associateWith {
                    TopicMetrics(
                        head = heads[it] ?: 0L,
                        offsets = offsets[it] ?: emptyMap()
                    )
                }
            }
        }
    }
}