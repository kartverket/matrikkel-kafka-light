package no.kartverket.matrikkel.kafkaclient

import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okio.Buffer
import org.junit.jupiter.api.Assertions.fail
import java.util.concurrent.TimeUnit

inline fun <reified T> MockWebServer.enqueueCborResponse(body: T, code: Int = 200) {
    enqueue(
        MockResponse().setResponseCode(code).setHeader("Content-Type", "application/cbor")
            .setBody(Buffer().write(Cbor.encodeToByteArray(body)))
    )
}

fun MockWebServer.takeRequests(n: Int): List<RecordedRequest> {
    var counter = 0
    return buildList {
        repeat(n) {
            add(
                takeRequest(2, TimeUnit.SECONDS)
                    ?: fail("Could not grab http request within 2 seconds. Failed after $counter")
            )
        }
    }
}

inline fun <reified T> RecordedRequest.responseBody(): T {
    val bodyBytes = body.readByteArray()
    return Cbor.decodeFromByteArray<T>(bodyBytes)
}