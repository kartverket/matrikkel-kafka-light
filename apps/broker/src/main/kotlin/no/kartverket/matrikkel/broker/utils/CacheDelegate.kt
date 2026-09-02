package no.kartverket.matrikkel.broker.utils

import java.time.Duration
import java.time.Instant
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty

fun <T : Any> cache(
    ttl: Duration,
    eager: Boolean = false,
    initializer: () -> T
): CacheDelegate<T> = CacheDelegate(ttl, eager, initializer)

class CacheDelegate<T : Any>(
    private val ttl: Duration,
    eager: Boolean = false,
    private val initializer: () -> T
) : ReadOnlyProperty<Any?, T> {
    @Volatile private var _value: T? = null
    @Volatile private var _lastComputed: Instant? = null

    init {
        if (eager) {
            synchronized(this) {
                val computed = initializer()
                _value = computed
                _lastComputed = Instant.now()
            }
        }
    }

    override fun getValue(thisRef: Any?, property: KProperty<*>): T {
        val now = Instant.now()
        val v1 = _value
        val lastComputed = _lastComputed

        if (v1 == null || lastComputed == null || lastComputed + ttl < now) {
            return synchronized(this) {
                val computed = initializer()
                _value = computed
                _lastComputed = now
                computed
            }
        }

        return v1
    }
}