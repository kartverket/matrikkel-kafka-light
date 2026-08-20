package no.kartverket.matrikkel.broker.integration

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.http.*
import io.ktor.serialization.kotlinx.cbor.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.testing.*
import kotlinx.serialization.cbor.Cbor
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
                name = "seciond-topic", acl = TopicAccessControlList(
                    publishIdentities = setOf(),
                    consumeIdentities = setOf(),
                )
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

    private fun WithDatabase.runIntegrationTest(
        block: suspend ApplicationTestBuilder.(ProducerFactory, ConsumerFactory) -> Unit
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
                    install(ContentNegotiation) {
                        cbor(
                            Cbor {
                                ignoreUnknownKeys = true
                                encodeDefaults = true
                            }
                        )
                    }

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
                        instanceId = "instance-1",
                        initialOffsetPolicy = InitialOffsetPolicy.EARLIEST,
                        correlationIdProvider = { UUID.randomUUID().toString() }
                    ),
                    client = clientFactory(username)
                )
            }

            block(producerFactory, consumerFactory)
        }
    }

    private suspend fun <TKey, TValue> MessageProducer<TKey, TValue>.sendSync(record: ProducerRecord<TKey, TValue>): Unit {
        this.send(record).join()
    }
}