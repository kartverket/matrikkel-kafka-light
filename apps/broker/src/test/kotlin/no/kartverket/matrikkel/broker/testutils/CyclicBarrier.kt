package no.kartverket.matrikkel.broker.testutils

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class CyclicBarrier(
    private val parties: Int,
) {
    init {
        require(parties > 0) {
            "parties must be greater than zero"
        }
    }

    private val mutex = Mutex()
    private var remaining = parties
    private var generation = CompletableDeferred<Unit>()

    suspend fun await() {
        val currentGeneration = mutex.withLock {
            val currentGeneration = generation

            remaining--

            if (remaining == 0) {
                // Prepare the barrier for its next cycle before releasing
                // the callers waiting in the current cycle.
                remaining = parties
                generation = CompletableDeferred()
                currentGeneration.complete(Unit)
            }

            currentGeneration
        }

        currentGeneration.await()
    }
}