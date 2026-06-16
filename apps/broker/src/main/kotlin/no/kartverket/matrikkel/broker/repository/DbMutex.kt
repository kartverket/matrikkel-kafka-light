package no.kartverket.matrikkel.broker.repository

import kotliquery.TransactionalSession
import kotliquery.queryOf

object DbMutex {
    interface LockScope {
        val seed: Long
    }
    class DbLockAcquired internal constructor()

    context(tx: TransactionalSession)
    fun lock(scope: LockScope, name: String) {
        val query = queryOf(
            "select pg_advisory_xact_lock(hashtextextended(:name, :seed))",
            mapOf(
                "name" to name,
                "seed" to scope.seed
            )
        ).asExecute

        tx.run(query)
    }

    context(tx: TransactionalSession)
    fun <T> withLock(scope: LockScope, name: String, block: context(DbLockAcquired) () -> T): T {
        lock(scope, name)
        return with(DbLockAcquired()) {
            block()
        }
    }
}