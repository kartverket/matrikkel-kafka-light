package no.kartverket.matrikkel.broker.domain

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

class CleanupPolicyTest {

    @Test
    fun `policies reject non-positive durations`() {
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Delete(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Compact(minCompactionLag = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Compact(tombstoneRetention = (-1).hours) }
    }

}