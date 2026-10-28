package no.kartverket.matrikkel.kafkaclient

import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.uuid.Uuid
import kotlin.uuid.getUuid
import kotlin.uuid.putUuid
import kotlin.uuid.toJavaUuid
import kotlin.uuid.toKotlinUuid

interface Serde<T> {
    fun serialize(data: T): ByteArray
    fun deserialize(data: ByteArray): T
}

inline fun <reified T> jsonSerde(): Serde<T> {
    val json = Json {
        ignoreUnknownKeys = true
    }

    return object : Serde<T> {
        override fun serialize(data: T): ByteArray {
            return json.encodeToString(serializer<T>(), data).encodeToByteArray()
        }

        override fun deserialize(data: ByteArray): T {
            return json.decodeFromString(serializer<T>(), data.decodeToString())
        }
    }
}

object StringSerde : Serde<String> {
    override fun serialize(data: String): ByteArray = data.encodeToByteArray()
    override fun deserialize(data: ByteArray): String = data.decodeToString()
}

object IntSerde : Serde<Int> {
    override fun serialize(data: Int): ByteArray = ByteBuffer.allocate(Int.SIZE_BYTES).putInt(data).array()
    override fun deserialize(data: ByteArray): Int = ByteBuffer.wrap(data).getInt()
}

object LongSerde : Serde<Long> {
    override fun serialize(data: Long): ByteArray = ByteBuffer.allocate(Long.SIZE_BYTES).putLong(data).array()
    override fun deserialize(data: ByteArray): Long = ByteBuffer.wrap(data).getLong()
}

object FloatSerde : Serde<Float> {
    override fun serialize(data: Float): ByteArray = ByteBuffer.allocate(Float.SIZE_BYTES).putFloat(data).array()
    override fun deserialize(data: ByteArray): Float = ByteBuffer.wrap(data).getFloat()
}

object DoubleSerde : Serde<Double> {
    override fun serialize(data: Double): ByteArray = ByteBuffer.allocate(Double.SIZE_BYTES).putDouble(data).array()
    override fun deserialize(data: ByteArray): Double = ByteBuffer.wrap(data).getDouble()
}

object UuidSerde : Serde<Uuid> {
    override fun serialize(data: Uuid): ByteArray = ByteBuffer.allocate(Uuid.SIZE_BYTES).putUuid(data).array()
    override fun deserialize(data: ByteArray): Uuid = ByteBuffer.wrap(data).getUuid()
}

object UUIDSerde : Serde<UUID> {
    override fun serialize(data: UUID): ByteArray = UuidSerde.serialize(data.toKotlinUuid())
    override fun deserialize(data: ByteArray): UUID = UuidSerde.deserialize(data).toJavaUuid()
}