package no.kartverket.matrikkel.broker.domain

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

sealed interface CleanupPolicy {
    /** Delete records older than [retention]. */
    data class Delete(val retention: Duration) : CleanupPolicy {
        init {
            require(retention > Duration.ZERO) { "Delete retention must be positive" }
        }
    }

    /**
     * Keep only the latest record per key. Records younger than [minCompactionLag] are never compacted.
     * Tombstones are removed after [tombstoneRetention].
     */
    data class Compact(
        val minCompactionLag: Duration = 24.hours,
        val tombstoneRetention: Duration = 24.hours,
    ) : CleanupPolicy {
        init {
            require(minCompactionLag > Duration.ZERO) { "Compact minCompactionLag must be positive" }
            require(tombstoneRetention > Duration.ZERO) { "Compact tombstoneRetention must be positive" }
        }
    }
}
