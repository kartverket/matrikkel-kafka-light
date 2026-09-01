package no.kartverket.no.kartverket.matrikkel.broker.utils

import assertk.assertThat
import assertk.assertions.contains
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.matrikkel.broker.repository.withTransaction
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.withLease
import no.kartverket.matrikkel.broker.service.records.OffsetRepository
import no.kartverket.matrikkel.broker.service.records.RecordsRepository
import no.kartverket.matrikkel.broker.utils.TopicMetricsBinder
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.uuid.Uuid

class TopicMetricsBinderTest : WithDatabase {
    val TestLock: DbMutex.LockScope = object : DbMutex.LockScope {
        override val seed: Long = 27597612847L
    }

    val topic1 = Topic(
        name = "first-topic", acl = TopicAccessControlList(
            publishIdentities = setOf(),
            consumeIdentities = setOf(),
        )
    )
    val topic2 = Topic(
        name = "second-topic", acl = TopicAccessControlList(
            publishIdentities = setOf(),
            consumeIdentities = setOf(),
        )
    )

    val topicCatalog = TopicCatalog(
        topics = listOf(topic1, topic2)
    )

    @Test
    fun `scraping fetches data from database automagically`() = runTest { registry ->
        dataSource().withTransaction {
            withLease(topic1, "consumer1", "consumer1") {
                DbMutex.withLock(TestLock, topic1.name) {
                    OffsetRepository.getOffset(topic1, "consumer1", InitialOffsetPolicy.LATEST)
                    OffsetRepository.setOffset(topic1, "consumer1", 123L)
                    RecordsRepository.insertRecords(
                        topic = topic1,
                        identity = ServiceIdentity("tester"),
                        correlationId = Uuid.random(),
                        request = PublishRequest(
                            idempotencyKey = "test",
                            records = listOf(
                                PublishRecord(
                                    key = byteArrayOf(),
                                    payload = byteArrayOf(),
                                )
                            )
                        ),
                        initialSequence = 456L,
                    )
                }
            }
        }

        val content = registry.scrape()

        assertThat(content).contains("consumer_offset{consumer_group=\"consumer1\",topic=\"first-topic\"} 123.0")
        assertThat(content).contains("topic_head{topic=\"first-topic\"} 457.0")
        assertThat(content).contains("topic_head{topic=\"second-topic\"} 0.0")
    }

    private fun WithDatabase.runTest(block: suspend (PrometheusMeterRegistry) -> Unit) {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

        TopicMetricsBinder(topicCatalog, dataSource()).bindTo(registry)

        runBlocking {
            block(registry)
        }
    }
}
