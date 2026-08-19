package no.kartverket.matrikkel.kafkaclient

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.cbor.*
import kotlinx.coroutines.delay
import kotlinx.serialization.cbor.Cbor
import java.io.Closeable
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/**
 * These do not go over the wire since they carry type-information.
 * Thus, the client needs to map the over-the-wire representation into ConsumerRecords
 */
data class ConsumerRecords<TKey, TValue>(
    val topic: String,
    val records: List<ConsumerRecord<TKey, TValue>>,
)
data class ConsumerRecord<TKey, TValue>(
    val topic: String,
    val sequence: Long,
    val key: TKey,
    val value: TValue? = null,
    val publishedAt: Instant,
)

interface MessageConsumer<TKey, TValue> : Closeable {
    suspend fun poll(maxRecords: Int? = null, timeout: Duration? = null): ConsumerRecords<TKey, TValue>
    suspend fun commitSync(sequence: Long): CommitResponse
    suspend fun seek(sequence: Long): SeekResponse
    suspend fun heartbeat(): HeartbeatResponse

    data class Config<TKey, TValue>(
        val server: Url,
        val topic: String,
        val keySerializer: Serde<TKey>,
        val valueSerializer: Serde<TValue>,
        val correlationIdProvider: () -> String,
        val maxRetries: Int = 5,
        val consumerGroup: String,
        val instanceId: String,
        val timeout: Duration = 10.seconds,
        val maxRecords: Int = 100,
        val initialOffsetPolicy: InitialOffsetPolicy
    )

    class Impl<TKey, TValue>(
        private val config: Config<TKey, TValue>,
    ) : MessageConsumer<TKey, TValue> {
        private var leaseToken: String? = null
        private val client = HttpClient(CIO) {
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
                retryOnServerErrors(maxRetries = config.maxRetries)
                exponentialDelay()
            }
        }

        override suspend fun poll(
            maxRecords: Int?,
            timeout: Duration?,
        ): ConsumerRecords<TKey, TValue> {
            val response = try {
                client.postCBOR(
                    operation = "poll",
                    body = PollRequest(
                        maxRecords = maxRecords ?: config.maxRecords,
                        consumerGroup = config.consumerGroup,
                        instanceId = config.instanceId,
                        initialOffsetPolicy = config.initialOffsetPolicy,
                    )
                )
            } catch (e: ClientRequestException) {
                when (e.response.status) {
                    HttpStatusCode.Locked -> {
                        delay(timeout ?: config.timeout)
                        return ConsumerRecords(topic = config.topic, records = emptyList())
                    }
                    else -> throw e
                }
            }

            val responseBody = response.body<PollResponse>()
            this.leaseToken = responseBody.leaseToken

            return ConsumerRecords(topic = config.topic, records = responseBody.records.map {
                ConsumerRecord(
                    topic = config.topic,
                    sequence = it.sequence,
                    key = config.keySerializer.deserialize(it.key),
                    value = it.payload?.let(config.valueSerializer::deserialize),
                    publishedAt = it.publishedAt,
                )
            })

        }

        override suspend fun commitSync(sequence: Long): CommitResponse {
            if (leaseToken == null) error("leaseToken is null")
            TODO("Not yet implemented")
        }

        override suspend fun seek(sequence: Long): SeekResponse {
            TODO("Not yet implemented")
        }

        override suspend fun heartbeat(): HeartbeatResponse {
            TODO("Not yet implemented")
        }

        override fun close() {
            client.close()
        }

        private suspend inline fun <reified T> HttpClient.postCBOR(
            operation: String,
            body: T,
        ): HttpResponse {
            return this.post {
                url.takeFrom(config.server)
                    .appendPathSegments("topics", config.topic, operation)
                header(HttpHeaders.XCorrelationId, config.correlationIdProvider())
                accept(ContentType.Application.Cbor)
                contentType(ContentType.Application.Cbor)
                setBody(body)
            }
        }
    }
}