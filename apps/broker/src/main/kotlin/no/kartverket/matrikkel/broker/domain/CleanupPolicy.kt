package no.kartverket.matrikkel.broker.domain

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

sealed interface CleanupPolicy {
    /** Delete records older than [retention]. */
    data class Delete(val retention: Duration) : CleanupPolicy

    /** Keep only the latest record per key. Tombstones are removed after [tombstoneRetention]. */
    data class Compact(val tombstoneRetention: Duration = 24.hours) : CleanupPolicy
}
