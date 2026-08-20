package no.kartverket.matrikkel.kafkaclient

import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.io.Closeable
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.uuid.Uuid

/**
 * This does not go over the wire since it carries type-information.
 * Thus, the client needs to map the records into a over-the-wire representation
 */
data class ProducerRecord<TKey, TValue>(
    val key: TKey,
    val value: TValue? = null,
)

interface MessageProducer<TKey, TValue> : Closeable {
    suspend fun send(record: ProducerRecord<TKey, TValue>): CompletableDeferred<Unit>

    data class Config<TKey, TValue>(
        val server: Url,
        val authentication: ClientAuthentication? = null,
        val topic: String,
        val keySerializer: Serde<TKey>,
        val valueSerializer: Serde<TValue>,
        val correlationIdProvider: () -> String,
        val bufferSize: Int = 100,
        val linger: Duration = 100.milliseconds,
        val maxRetries: Int = 5,
    )

    class Impl<TKey, TValue>(
        private val config: Config<TKey, TValue>,
        private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
        private val client: HttpClient = createHttpClient(
            maxRetries = config.maxRetries,
            authentication = config.authentication,
        ),
    ) : MessageProducer<TKey, TValue> {
        private data class PendingRecord(
            val record: PublishRecord,
            val result: CompletableDeferred<Unit>
        )

        private val queue = Channel<PendingRecord>(capacity = Channel.UNLIMITED)

        private val senderJob = scope.launch {
            while (true) {
                val batch = nextBatch() ?: break
                publish(batch)
            }
        }

        override suspend fun send(record: ProducerRecord<TKey, TValue>): CompletableDeferred<Unit> {
            val callback = CompletableDeferred<Unit>()
            queue.send(
                PendingRecord(
                    PublishRecord(
                        key = config.keySerializer.serialize(record.key),
                        payload = record.value?.let(config.valueSerializer::serialize),
                    ),
                    callback
                )
            )
            return callback
        }

        override fun close() {
            queue.close()
            runBlocking {
                senderJob.join()
                client.close()
                scope.cancel()
            }
        }

        private suspend fun nextBatch(): List<PendingRecord>? {
            val first = queue.receiveCatching().getOrNull() ?: return null
            val batch = ArrayList<PendingRecord>(config.bufferSize)
            batch += first

            withTimeoutOrNull(config.linger) {
                while (batch.size < config.bufferSize) {
                    val next = queue.receiveCatching().getOrNull() ?: return@withTimeoutOrNull
                    batch += next
                }
            }

            return batch
        }


        private suspend fun publish(batch: List<PendingRecord>) {
            try {
                httpPublish(batch.map(PendingRecord::record))
                batch.forEach { it.result.complete(Unit) }
            } catch (ex: Exception) {
                batch.forEach { it.result.completeExceptionally(ex) }
            }
        }

        private suspend fun httpPublish(batch: List<PublishRecord>) {
            val idempotencyKey = Uuid.random().toString()
            val correlationId = config.correlationIdProvider()

            client.post {
                url.takeFrom(config.server)
                    .appendPathSegments("topics", config.topic, "publish")
                header(HttpHeaders.XCorrelationId, correlationId)
                accept(ContentType.Application.Cbor)
                contentType(ContentType.Application.Cbor)
                setBody(
                    PublishRequest(
                        idempotencyKey = idempotencyKey,
                        records = batch,
                    )
                )
            }
        }
    }
}