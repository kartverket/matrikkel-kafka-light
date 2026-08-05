package no.kartverket.no.kartverket.matrikkel.broker.api

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.prop
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.cbor.cbor
import io.ktor.server.auth.*
import io.ktor.server.testing.*
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.serialization.cbor.Cbor
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.matrikkel.broker.ErrorResponse
import no.kartverket.matrikkel.broker.api.topicRoutes
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.standardPlugins
import no.kartverket.matrikkel.kafkaclient.PublishRecord
import no.kartverket.matrikkel.kafkaclient.PublishRequest
import no.kartverket.matrikkel.kafkaclient.PublishResponse
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.uuid.Uuid

class TopicRoutesTest {
    val security = Security(
        Security.AuthProvider(
            name = "azuread",
            jwksConfig = Security.JwksConfig.OidcWellkownUrl("https://dummy.uri"),
            tokenLocation = Security.TokenLocation.Header(HttpHeaders.Authorization),
        )
    )
    val topicCatalog = TopicCatalog(
        topics = listOf(
            Topic(
                name = "test-topic", acl = TopicAccessControlList(
                    publishIdentities = setOf(TopicAccessControlList.WILDCARD),
                    consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
                )
            ),
            Topic(
                name = "locked-topic", acl = TopicAccessControlList(
                    publishIdentities = setOf(),
                    consumeIdentities = setOf(),
                )
            ),
        )
    )
    val request = PublishRequest(
        idempotencyKey = "idempotency_key",
        records = listOf(
            PublishRecord(
                key = "record_key".toByteArray(),
                payload = "payload".toByteArray(),
            )
        )
    )

    @Nested
    inner class PublishEndpoint {

        @Test
        fun `should return error for missing topic`() {
            topicRouteTest { client, _ ->
                val response = client.post("/topics/missing-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("missing_topic")
            }
        }

        @Test
        fun `should return error for missing service identity`() {
            topicRouteTest(disableAuth = true) { client, _ ->
                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("unauthorized")
            }
        }

        @Test
        fun `should return error for missing correlationId header`() {
            topicRouteTest { client, _ ->
                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("invalid_request")
            }
        }

        @Test
        fun `should return forbidding if ACL denies access`() {
            topicRouteTest { client, _ ->
                val response = client.post("/topics/locked-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("forbidden")
            }
        }

        @Test
        fun `should return bad_request if record_key is invalid`() {
            topicRouteTest { client, _ ->
                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request.copy(
                        records = request.records.map { it.copy(key = "".toByteArray()) }
                    ))
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("invalid_request")
            }
        }

        @Test
        fun `should return bad_request if idempotency_key is invalid`() {
            topicRouteTest { client, _ ->
                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request.copy(idempotencyKey = ""))
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("invalid_request")
            }
        }

        @Test
        fun `should return success response from service`() {
            topicRouteTest { client, mockService ->
                coEvery { mockService.publish(any(), any()) } returns
                        Result.success(
                            PublishResponse(
                                topic = topicCatalog.topics.first().name,
                                sequence = 123L,
                                idempotencyKey = "idempotency_key",
                                publishedAt = Clock.System.now(),
                            )
                        )

                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<PublishResponse>()
                assertThat(result).isNotNull().all {
                    prop(PublishResponse::topic).isEqualTo("test-topic")
                    prop(PublishResponse::sequence).isEqualTo(123L)
                    prop(PublishResponse::idempotencyKey).isEqualTo("idempotency_key")
                }
            }
        }

        @Test
        fun `should return error response from service`() {
            topicRouteTest { client, mockService ->
                coEvery { mockService.publish(any(), any()) } returns
                        Result.failure(Exception("Something went wrong"))

                val response = client.post("/topics/test-topic/publish") {
                    header(HttpHeaders.XCorrelationId, Uuid.random().toString())
                    header(HttpHeaders.ContentType, ContentType.Application.Cbor)
                    setBody(request)
                }

                val result = response.body<ErrorResponse>()
                assertThat(result.code).isEqualTo("internal_error")
            }
        }
    }

    @Nested
    inner class PollEndpoint {

    }



    private fun topicRouteTest(
        disableAuth: Boolean = false, block: suspend ApplicationTestBuilder.(HttpClient, Records.Service) -> Unit
    ) {
        return testApplication {
            val recordsService = mockk<Records.Service>()

            install(Authentication) {
                if (!disableAuth) {
                    security.setupMock("fake-user")
                }
            }

            application {
                standardPlugins("testing")
            }

            routing {
                if (disableAuth) {
                    topicRoutes(topicCatalog, recordsService)
                } else {
                    authenticate(*security.authproviders) {
                        topicRoutes(topicCatalog, recordsService)
                    }
                }
            }

            client = createClient {
                install(ContentNegotiation) {
                    cbor(
                        Cbor {
                            ignoreUnknownKeys = true
                            encodeDefaults = true
                        }
                    )
                }
            }

            block(client, recordsService)
        }
    }
}

