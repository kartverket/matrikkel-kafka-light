package no.kartverket.matrikkel.kafkaclient

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNull
import io.ktor.client.plugins.ClientRequestException
import io.ktor.client.plugins.ServerResponseException
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds

class MessageConsumerTest {
    private val server = MockWebServer()
    private val clientConfig = MessageConsumer.Config(
        server = Url(server.url("/").toString()),
        topic = "test-topic",
        keySerializer = StringSerde,
        valueSerializer = StringSerde,
        correlationIdProvider = { "test-correlation-id" },
        consumerGroup = "test-group",
        instanceId = "test-instance-id",
        initialOffsetPolicy = InitialOffsetPolicy.EARLIEST,
    )

    private val publishedAt = Clock.System.now()

    val pollRecord = PollRecord(
        key = "my-key".encodeToByteArray(),
        payload = "my-value".encodeToByteArray(),
        sequence = 1L,
        publishedAt = publishedAt,
    )
    private val pollResponse = PollResponse(
        leaseToken = "test-lease-token",
        records = listOf(
            pollRecord
        ),
    )


    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun `check for correct request data with default config`() = runTest {
        server.enqueueCborResponse(pollResponse)
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        consumer.poll()
        consumer.close()

        val request = server.takeRequests(1).first()
        assertThat(request.method).isEqualTo("POST")
        assertThat(request.path).isEqualTo("/topics/test-topic/poll")
        assertThat(request.getHeader(HttpHeaders.XCorrelationId)).isEqualTo("test-correlation-id")
        assertThat(request.getHeader(HttpHeaders.Accept)).isEqualTo("application/cbor")
        assertThat(request.getHeader(HttpHeaders.ContentType)).isEqualTo("application/cbor")
    }

    @Test
    fun `check for correct request body`() = runTest {
        server.enqueueCborResponse(pollResponse)
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        consumer.poll()
        consumer.close()

        val request = server.takeRequests(1).first()
        val body = request.responseBody<PollRequest>()
        assertThat(body.consumerGroup).isEqualTo("test-group")
        assertThat(body.instanceId).isEqualTo("test-instance-id")
        assertThat(body.maxRecords).isEqualTo(100)
        assertThat(body.initialOffsetPolicy).isEqualTo(InitialOffsetPolicy.EARLIEST)
    }

    @Test
    fun `should use maxRecords when set`() = runTest {
        server.enqueueCborResponse(pollResponse)
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        consumer.poll(maxRecords = 5)
        consumer.close()

        val request = server.takeRequests(1).first()
        val body = request.responseBody<PollRequest>()
        assertThat(body.consumerGroup).isEqualTo("test-group")
        assertThat(body.instanceId).isEqualTo("test-instance-id")
        assertThat(body.maxRecords).isEqualTo(5)
    }

    @Test
    fun `check that consumer record is mapped correctly`() = runTest {
        server.enqueueCborResponse(pollResponse)
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        val pollResult = consumer.poll()
        consumer.close()

        assertThat(pollResult.topic).isEqualTo("test-topic")
        assertThat(pollResult.records).hasSize(1)
        val firstRecord = pollResult.records.first()
        assertThat(firstRecord.topic).isEqualTo("test-topic")
        assertThat(firstRecord.key).isEqualTo("my-key")
        assertThat(firstRecord.value).isEqualTo("my-value")
        assertThat(firstRecord.sequence).isEqualTo(1L)
        assertThat(firstRecord.publishedAt).isEqualTo(publishedAt)
    }

    @Test
    fun `null payload maps to null value`() = runTest {
        server.enqueueCborResponse(pollResponse.copy(records = listOf(pollRecord.copy(payload = null))))
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        val pollResult = consumer.poll()
        consumer.close()

        assertThat(pollResult.topic).isEqualTo("test-topic")
        assertThat(pollResult.records).hasSize(1)
        val firstRecord = pollResult.records.first()
        assertThat(firstRecord.topic).isEqualTo("test-topic")
        assertThat(firstRecord.key).isEqualTo("my-key")
        assertThat(firstRecord.value).isNull()
    }

    @Test
    fun `empty pollresponse list returns empty consumer records list`() = runTest {
        server.enqueueCborResponse(pollResponse.copy(records = emptyList()))
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        val pollResult = consumer.poll()
        consumer.close()

        assertThat(pollResult.topic).isEqualTo("test-topic")
        assertThat(pollResult.records).isEmpty()
    }

    @Test
    fun `should wait for default timeout if locked and return empty list`() = runTest {
        server.enqueue(MockResponse().setResponseCode(HttpStatusCode.Locked.value))
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        val pollResult = consumer.poll()
        consumer.close()

        assertThat(pollResult.topic).isEqualTo("test-topic")
        assertThat(pollResult.records).isEmpty()
        assertThat(currentTime).isEqualTo(clientConfig.timeout.inWholeMilliseconds)
    }

    @Test
    fun `should wait for provided timeout instead of config default when locked`() = runTest {
        server.enqueue(MockResponse().setResponseCode(HttpStatusCode.Locked.value))
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        val newTimeout = 5.seconds
        consumer.poll(timeout = newTimeout)
        consumer.close()

        assertThat(currentTime).isEqualTo(newTimeout.inWholeMilliseconds)
    }

    @Test
    fun `retries on server error`() = runTest {
        repeat(4) {
            server.enqueue(MockResponse().setResponseCode(500))
        }
        server.enqueueCborResponse(pollResponse)
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        consumer.poll()
        consumer.close()

        assertThat(server.requestCount).isEqualTo(5)
    }

    @Test
    fun `throws error after maxRetries is reached and still fails`() = runTest {
        repeat(3) {
            server.enqueue(MockResponse().setResponseCode(500))
        }
        server.start()

        val consumer = MessageConsumer.Impl(
            config = clientConfig.copy(
                maxRetries = 2,
            ),
        )

        assertFailure {
            consumer.poll()
        }.isInstanceOf<ServerResponseException>()

        consumer.close()

        assertThat(server.requestCount).isEqualTo(3)
    }

    @Test
    fun `should throw error on 400-errors other than 423-locked`() = runTest {
        server.enqueue(MockResponse().setResponseCode(400))
        server.start()

        val consumer = MessageConsumer.Impl(config = clientConfig)

        assertFailure {
            consumer.poll()
        }.isInstanceOf<ClientRequestException>()

        consumer.close()
    }
}