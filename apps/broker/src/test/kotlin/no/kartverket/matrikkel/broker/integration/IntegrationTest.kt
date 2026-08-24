package no.kartverket.matrikkel.broker.integration

import assertk.all
import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.each
import assertk.assertions.hasMessage
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isLessThan
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.testing.*
import kotlinx.coroutines.*
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.matrikkel.broker.api.topicRoutes
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.standardPlugins
import no.kartverket.matrikkel.kafkaclient.*
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.io.encoding.Base64
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTimedValue

private typealias ProducerFactory = (username: String, topic: String) -> MessageProducer<String, String>
private typealias ConsumerFactory = (username: String, topic: String) -> MessageConsumer<String, String>

class IntegrationTest : WithDatabase {
    val topicCatalog = TopicCatalog(
        topics = listOf(
            Topic(
                name = "first-topic", acl = TopicAccessControlList(
                    publishIdentities = setOf(TopicAccessControlList.WILDCARD),
                    consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
                )
            ),
            Topic(
                name = "second-topic", acl = TopicAccessControlList(
                    publishIdentities = setOf("p1", "p2"),
                    consumeIdentities = setOf("c1", "c2"),
                ),
                leaseTime = 2.seconds
            ),
        )
    )

    @Test
    fun `should be able to publish and consume messages`() {
        runIntegrationTest { producerFactory, consumerFactory ->
            val producer = producerFactory("p1", "first-topic")
            val consumer = consumerFactory("c1", "first-topic")

            producer.sendSync(ProducerRecord(key = "record-1", value = "content-1"))

            val records = consumer.poll()
            consumer.commitSync()

            producer.close()
            consumer.close()

            assertThat(records.records).hasSize(1)
            assertThat(records.records.first().value).isEqualTo("content-1")
        }
    }

    @Test
    fun `throughput test (10k records in two topics)`() {
        val messagesPerClient = 10_000
        val producerIds = listOf("p1" to "first-topic", "p2" to "second-topic")
        val consumerIds = listOf("c1" to "first-topic", "c2" to "second-topic")

        runIntegrationTest { producerFactory, consumerFactory ->
            val startupSignal = CompletableDeferred<Unit>()

            val producers = producerIds.map { (id, topic) ->
                launch(Dispatchers.IO) {
                    val producer = producerFactory(id, topic)
                    startupSignal.await()

                    producer.produceMessages {
                        repeat(messagesPerClient) {
                            yield(ProducerRecord("$id-$it", "$id-$it"))
                        }
                    }

                    producer.close()
                }
            }

            val consumers = consumerIds.map { (id, topic) ->
                async(Dispatchers.IO) {
                    val consumer = consumerFactory(id, topic)
                    startupSignal.await()
                    consumer.use { it.consumeMessages(messagesPerClient) }
                }
            }

            val (records, duration) = measureTimedValue {
                startupSignal.complete(Unit)

                joinAll(*producers.toTypedArray())
                awaitAll(*consumers.toTypedArray())
            }

            assertThat(records).each { recordsAssert ->
                recordsAssert
                    .transform { list -> list.map { it.sequence } }
                    .all {
                        hasSize(messagesPerClient)
                        isEqualTo((1L..messagesPerClient.toLong()).toList())
                    }
            }


            val numberOfRecords = records.sumOf { it.size }
            // Should be approx: Processed 20000 in ~2s
            println("Processed $numberOfRecords in $duration")

            assertThat(duration).isLessThan(5.seconds)
        }
    }

    @Test
    fun `multiple producers and consumers scenario`() {
        val messagesPerClient = 500
        val producerIds = listOf("p1" to "second-topic", "p2" to "second-topic")
        val consumerIds = listOf("c1" to "second-topic", "c2" to "second-topic")

        runIntegrationTest { producerFactory, consumerFactory ->
            val startupSignal = CompletableDeferred<Unit>()

            val producers = producerIds.map { (id, topic) ->
                launch(Dispatchers.IO) {
                    val producer = producerFactory(id, topic)
                    startupSignal.await()

                    producer.produceMessages {
                        repeat(messagesPerClient) {
                            yield(ProducerRecord("$id-$it", "$id-$it"))
                        }
                    }

                    producer.close()
                }
            }
            val consumers = consumerIds.map { (id, topic) ->
                async(Dispatchers.IO) {
                    val consumer = consumerFactory(id, topic)
                    startupSignal.await()
                    consumer.use { it.consumeMessages(messagesPerClient) }
                }
            }

            startupSignal.complete(Unit)

            joinAll(*producers.toTypedArray())
            val records = awaitAll(*consumers.toTypedArray())
            val allRecords = records.flatten()

            // Records delivered exactly once
            val expectedNumberOfRecords = messagesPerClient * producerIds.size
            assertThat(allRecords).hasSize(expectedNumberOfRecords)
            assertThat(allRecords.distinctBy { it.key }).hasSize(expectedNumberOfRecords)
            assertThat(allRecords.distinctBy { it.sequence }).hasSize(expectedNumberOfRecords)

            // The sequence has no gaps or duplicates
            val fullsequence = allRecords.map { it.sequence }.sorted()
            assertThat(fullsequence).isEqualTo((1..expectedNumberOfRecords.toLong()).toList())

            // Both consumers have received records
            assertThat(records).each {
                it.hasSize(messagesPerClient)
            }

            // No records was delivered to both consumers
            val (firstRecords, secondRecords) = records
            val firstRecordSeqs = firstRecords.map { it.sequence }.toSet()
            val secondRecordSeqs = secondRecords.map { it.sequence }.toSet()
            assertThat(firstRecordSeqs.intersect(secondRecordSeqs)).isEqualTo(emptySet())

            // Every consumer has seen a monotonically increasing sequence list
            for (consumerRecords in records) {
                val sequence = consumerRecords.map { it.sequence }
                assertThat(sequence).isEqualTo(sequence.sorted())
            }
        }
    }

    @Test
    fun `returns forbidden if consumer-producers tries to access topic`() {
        runIntegrationTest { producerFactory, consumerFactory ->
            producerFactory("illegal", "second-topic").use {
                assertFailure {
                    it.sendSync(ProducerRecord("key", "value"))
                }.hasMessage("Service identity not authorized to execute command on this topic")
            }

            consumerFactory("illegal", "second-topic").use {
                assertFailure {
                    it.poll()
                }.hasMessage("Service identity not authorized to execute command on this topic")
            }
        }
    }

    private suspend fun MessageProducer<String, String>.produceMessages(
        block: suspend SequenceScope<ProducerRecord<String, String>>.() -> Unit
    ): MessageProducer<String, String> {
        for (record in sequence(block)) {
            this.send(record)
        }
        return this
    }

    private suspend fun MessageConsumer<String, String>.consumeMessages(n: Int): List<ConsumerRecord<String, String>> {
        val records = mutableListOf<ConsumerRecord<String, String>>()
        do {
            val response = this.poll(maxRecords = min(1000, n - records.size))
            records.addAll(response.records)
            this.commitSync()
        } while (records.size < n)
        return records
    }

    private fun WithDatabase.runIntegrationTest(
        block: suspend CoroutineScope.(ProducerFactory, ConsumerFactory) -> Unit
    ) {
        val security = Security(
            Security.AuthProvider(
                name = "azuread",
                jwksConfig = Security.JwksConfig.OidcWellkownUrl("https://dummy.uri"),
                tokenLocation = Security.TokenLocation.Header(HttpHeaders.Authorization),
            )
        )

        val recordsService = Records.ServiceImpl(dataSource())

        return testApplication {
            install(Authentication) {
                basic("azuread") {
                    validate { credentials ->
                        val token = JWT.decode(JWT.create().withSubject(credentials.name).sign(Algorithm.none()))
                        JWTPrincipal(token)
                    }
                }
            }
            application {
                standardPlugins("testing")
            }
            routing {
                authenticate(*security.authproviders) {
                    topicRoutes(topicCatalog, recordsService)
                }
            }

            val clientFactory = { username: String ->
                createClient {
                    standardConfig()

                    val authHeader = "Basic ${Base64.encode("$username:$username".encodeToByteArray())}"
                    defaultRequest {
                        header(HttpHeaders.Authorization, authHeader)
                    }
                }
            }

            val producerFactory: ProducerFactory = { username, topic ->
                MessageProducer.Impl(
                    config = MessageProducer.Config(
                        server = Url(""),
                        topic = topic,
                        keySerializer = StringSerde,
                        valueSerializer = StringSerde,
                        correlationIdProvider = { UUID.randomUUID().toString() },
                    ),
                    client = clientFactory(username)
                )
            }

            val consumerFactory: ConsumerFactory = { username, topic ->
                MessageConsumer.Impl(
                    config = MessageConsumer.Config(
                        server = Url(""),
                        topic = topic,
                        keySerializer = StringSerde,
                        valueSerializer = StringSerde,
                        consumerGroup = "group-1",
                        instanceId = username,
                        initialOffsetPolicy = InitialOffsetPolicy.EARLIEST,
                        correlationIdProvider = { UUID.randomUUID().toString() },
                        timeout = 1.seconds
                    ),
                    client = clientFactory(username)
                )
            }

            coroutineScope {
                block(producerFactory, consumerFactory)
            }
        }
    }

    private suspend fun <TKey, TValue> MessageProducer<TKey, TValue>.sendSync(record: ProducerRecord<TKey, TValue>): Unit {
        this.send(record).await()
    }
}