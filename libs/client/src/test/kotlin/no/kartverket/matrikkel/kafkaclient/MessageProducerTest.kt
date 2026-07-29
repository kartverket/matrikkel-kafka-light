package no.kartverket.matrikkel.kafkaclient

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import io.ktor.http.*
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MessageProducerTest {
    private val server = MockWebServer()
    private val clientConfig = {
        MessageProducer.Config(
            server = Url(server.url("/").toString()),
            topic = "my-topic",
            keySerializer = StringSerde,
            valueSerializer = StringSerde,
            correlationIdProvider = { "test-correlation-id" },
        )

    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun `sends records in one batch when buffer size is reached`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 3,
                linger = 1.seconds,
            )
        )

        producer.send(ProducerRecord("key-1", "value-1"))
        producer.send(ProducerRecord("key-2", "value-2"))
        producer.send(ProducerRecord("key-3", "value-3"))

        producer.close()

        val request = server.takeRequests(1).first()

        assertThat(request.method).isEqualTo("POST")
        assertThat(request.path).isEqualTo("/topics/my-topic/publish")
        assertThat(request.getHeader(HttpHeaders.XCorrelationId)).isEqualTo("test-correlation-id")

        val body = request.responseBody<PublishRequest>()

        assertThat(body.records).hasSize(3)
        assertThat(body.records[0].key.decodeToString()).isEqualTo("key-1")
        assertThat(body.records[0].payload?.decodeToString()).isEqualTo("value-1")
        assertThat(server.requestCount).isEqualTo(1)
    }

    @Test
    fun `sends records after linger time even if batch is not full`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 100,
                linger = 50.milliseconds,
            )
        )

        producer.send(ProducerRecord("key-1", "value-1"))

        val request = server.takeRequests(1).first()

        val body = request.responseBody<PublishRequest>()

        assertThat(body.records).hasSize(1)
    }

    @Test
    fun `splits into multiple batches if more records are sent`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.enqueue(MockResponse().setResponseCode(200))
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 3,
            )
        )

        repeat(7) {
            producer.send(ProducerRecord("key-$it", "value-$it"))
        }
        producer.close()

        val requests = server.takeRequests(3)
        val bodies = requests.map { it.responseBody<PublishRequest>() }

        assertThat(bodies[0].records).hasSize(3)
        assertThat(bodies[1].records).hasSize(3)
        assertThat(bodies[2].records).hasSize(1)
    }

    @Test
    fun `concurrent sends are preserved`() = runTest {
        val bufferSize = 100
        val workers = 50
        val sendPerWorker = 10
        val expectedRequets = (workers * sendPerWorker) / bufferSize
        repeat(expectedRequets) {
            server.enqueue(MockResponse().setResponseCode(200))
        }

        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = bufferSize,
            )
        )

        val jobs = (0 until workers).map { worker ->
            launch {
                repeat(sendPerWorker) { index ->
                    producer.send(
                        ProducerRecord(
                            "key-$worker-$index",
                            "value-$worker-$index",
                        )
                    )
                }
            }
        }

        jobs.joinAll()
        producer.close()

        val requests = server.takeRequests(expectedRequets)
        val bodies = requests.map { it.responseBody<PublishRequest>() }
        val keys = bodies
            .flatMap { body -> body.records.map { it.key.decodeToString() } }
            .toSet()

        assertThat(keys).hasSize(workers * sendPerWorker)
    }

    @Test
    fun `closes flushes remaining records`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 100,
                linger = 50.seconds,
            )
        )

        producer.send(ProducerRecord("key-1", "value-1"))
        producer.close()

        val request = server.takeRequests(1).first()

        val body = request.responseBody<PublishRequest>()

        assertThat(body.records).hasSize(1)
    }

    @Test
    fun `does not send empty batches`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 100,
                linger = 50.seconds,
            )
        )

        producer.close()

        assertThat(server.requestCount).isEqualTo(0)
    }

    @Test
    fun `null payloads are correctly serialized`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 100,
                linger = 50.seconds,
            )
        )

        producer.send(ProducerRecord("key-1", null))
        producer.close()

        val request = server.takeRequests(1).first()
        val body = request.responseBody<PublishRequest>()

        assertThat(server.requestCount).isEqualTo(1)
        assertThat(body.records).hasSize(1)
        assertThat(body.records.first().payload).isEqualTo(null)
    }

    @Test
    fun `retries on server error`() = runTest {
        server.enqueue(MockResponse().setResponseCode(500))
        server.enqueue(MockResponse().setResponseCode(500))
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                maxRetries = 3
            )
        )

        producer.send(ProducerRecord("key-1", null))
        producer.close()

        assertThat(server.requestCount).isEqualTo(3)
    }

    @Test
    fun `send completes when record has been published`() = runTest {
        server.enqueue(MockResponse().setResponseCode(200))
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 1,
            )
        )

        val delivery = producer.send(
            ProducerRecord(
                key = "key-1",
                value = "value-1",
            )
        )

        delivery.await()

        val request = server.takeRequests(1).first()

        val body = request.responseBody<PublishRequest>()

        assertThat(body.records).hasSize(1)
        assertThat(body.records.single().key.decodeToString()).isEqualTo("key-1")
        assertThat(body.records.single().payload!!.decodeToString()).isEqualTo("value-1")

        producer.close()
    }

    @Test
    fun `send fails when publish fails`() = runTest {
        repeat(3) {
            server.enqueue(MockResponse().setResponseCode(500))
        }
        server.start()

        val producer = MessageProducer.Impl(
            clientConfig().copy(
                bufferSize = 1,
                maxRetries = 2,
            )
        )

        val delivery = producer.send(
            ProducerRecord(
                key = "key-1",
                value = "value-1",
            )
        )

        assertFailure {
            delivery.await()
        }

        assertThat(server.requestCount).isEqualTo(3)

        producer.close()
    }

    private fun MockWebServer.takeRequests(n: Int): List<RecordedRequest> {
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

    private inline fun <reified T> RecordedRequest.responseBody(): T {
        val bodyBytes = body.readByteArray()
        return Cbor.decodeFromByteArray<T>(bodyBytes)
    }
}