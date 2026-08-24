package no.kartverket.matrikkel.kafkaclient

import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngineConfig
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRequestRetry
import io.ktor.client.plugins.HttpResponseValidator
import io.ktor.client.plugins.ResponseException
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.cbor.cbor
import kotlinx.serialization.cbor.Cbor

class KafkaClientException(
    val status: HttpStatusCode,
    val code: String,
    message: String,
    cause: Throwable,
) : RuntimeException(message, cause)

internal fun createHttpClient(
    maxRetries: Int = 0,
    authentication: ClientAuthentication? = null,
): HttpClient {
    return HttpClient(CIO) {
        standardConfig()

        install(HttpRequestRetry) {
            retryOnServerErrors(maxRetries = maxRetries)
            exponentialDelay()
        }

        if (authentication != null) {
            defaultRequest {
                header(HttpHeaders.Authorization, authentication.getAuthenticationHeaderValue())
            }
        }
    }
}

fun HttpClientConfig<out HttpClientEngineConfig>.standardConfig() {
    expectSuccess = true

    install(ContentNegotiation) {
        cbor(
            Cbor {
                ignoreUnknownKeys = true
                encodeDefaults = true
            }
        )
    }

    HttpResponseValidator {
        handleResponseException { cause ->
            val responseException = cause as? ResponseException
                ?: return@handleResponseException

            val error = runCatching {
                responseException.response.body<ErrorResponse>()
            }.getOrNull() ?: return@handleResponseException

            throw KafkaClientException(
                status = responseException.response.status,
                code = error.code,
                message = error.message,
                cause = cause
            )
        }
    }
}