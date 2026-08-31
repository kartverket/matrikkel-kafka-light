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
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class TopicMetricsTest : WithDatabase {

    val topic = Topic(
        name = "dummy_topic",
        acl = TopicAccessControlList(
            publishIdentities = setOf(TopicAccessControlList.WILDCARD),
            consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
        ),
        leaseTime = 30.seconds,
    )
    lateinit var metrics: TopicMetrics
    lateinit var registry: SimpleMeterRegistry
    val recordsRepository: RecordsRepository = mockk()

    @BeforeEach
    fun init() {
        registry = SimpleMeterRegistry()
        metrics = TopicMetrics(registry)
    }

    @Test
    fun initialize() {
        every {
            with(any<Session>()) {
                recordsRepository.currentHeadForTopic(any())
            }
        } returns 1

        runBlocking { metrics.initialize(listOf(topic), recordsRepository, dataSource()) }

        val metric = registry.find("topic.head").tags(
            "topic", "dummy_topic",
        ).gauge()
        assertThat(metric).isNotNull()
        assertThat(metric!!.value()).isEqualTo(1.0)
    }

    @Test
    fun update() {
        metrics.update(topic, 1)

        val metric = registry.find("topic.head").tags(
            "topic", "dummy_topic",
        ).gauge()
        assertThat(metric).isNotNull()
        assertThat(metric!!.value()).isEqualTo(1.0)

        metrics.update(topic, 2)
        assertThat(metric.value()).isEqualTo(2.0)
    }

}