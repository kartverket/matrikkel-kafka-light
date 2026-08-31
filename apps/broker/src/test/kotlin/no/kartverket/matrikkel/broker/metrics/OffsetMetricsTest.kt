package no.kartverket.matrikkel.broker.metrics

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotliquery.Session
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class OffsetMetricsTest : WithDatabase {

    val topic = Topic(
        name = "dummy_topic",
        acl = TopicAccessControlList(
            publishIdentities = setOf(TopicAccessControlList.WILDCARD),
            consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
        ),
        leaseTime = 30.seconds,
    )
    lateinit var metrics: OffsetMetrics
    lateinit var registry: SimpleMeterRegistry
    val offsetrepository: OffsetRepository = mockk()

    @BeforeEach
    fun init() {
        registry = SimpleMeterRegistry()
        metrics = OffsetMetrics(registry)
    }

    @Test
    fun initialize() {
        every {
            with(any<Session>()) {
                offsetrepository.getOffsets(any())
            }
        } returns mapOf("testconsumer" to 1)

        runBlocking { metrics.initialize(listOf(topic), offsetrepository, dataSource()) }

        val metric = registry.find("consumer.offset").tags(
            "topic", "dummy_topic",
            "consumerGroup", "testconsumer",
        ).gauge()
        assertThat(metric).isNotNull()
        assertThat(metric!!.value()).isEqualTo(1.0)
    }

    @Test
    fun update() {
        metrics.update(topic, "testconsumer", 1)

        val metric = registry.find("consumer.offset").tags(
            "topic", "dummy_topic",
            "consumerGroup", "testconsumer",
        ).gauge()
        assertThat(metric).isNotNull()
        assertThat(metric!!.value()).isEqualTo(1.0)

        metrics.update(topic, "testconsumer", 2)
        assertThat(metric.value()).isEqualTo(2.0)
    }

}