package no.kartverket.matrikkel.kafkaclient

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRequestRetry
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.kotlinx.cbor.cbor
import kotlinx.serialization.cbor.Cbor

fun createHttpClient(
    maxRetries: Int,
): HttpClient {
    return HttpClient(CIO) {
        expectSuccess = true
        install(ContentNegotiation) {
            cbor(
                Cbor {
                    ignoreUnknownKeys = true
                    encodeDefaults = true
                }
            )
        }

        install(HttpRequestRetry) {
            retryOnServerErrors(maxRetries = maxRetries)
            exponentialDelay()
        }
    }

}