package no.kartverket.matrikkel.broker.utils

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.ProxyBuilder
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.http.Url
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.net.URI

class OidcClient(val wellKnownUrl: String) {
    companion object {
        private val log = LoggerFactory.getLogger(OidcClient::class.java)
        private val CIO_ENGINE =
            CIO.create {
                val httpProxy = System.getenv("HTTP_PROXY")
                httpProxy?.let { proxy = ProxyBuilder.http(Url(it)) }
            }
    }

    private val httpClient =
        HttpClient(CIO_ENGINE) {
            install(ContentNegotiation) {
                json(
                    Json {
                        ignoreUnknownKeys = true
                    },
                )
            }
        }

    @Serializable
    class OidcDiscoveryConfig(
        @SerialName("jwks_uri") val jwksUrl: String,
        @SerialName("issuer") val issuer: String,
        @SerialName("authorization_endpoint") val authorizationEndpoint: String,
        @SerialName("token_endpoint") val tokenEndpoint: String,
    )

    suspend fun fetch(): OidcDiscoveryConfig =
        httpClient
            .runCatching { get(URI(wellKnownUrl).toURL()).body<OidcDiscoveryConfig>() }
            .onFailure { log.error("COuld not fetch oidc-config from $wellKnownUrl", it) }
            .getOrThrow()


}