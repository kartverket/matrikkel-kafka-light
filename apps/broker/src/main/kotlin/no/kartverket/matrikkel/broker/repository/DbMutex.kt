package no.kartverket.matrikkel.broker.repository

import kotliquery.Session
import kotliquery.queryOf

object DbMutex {
    interface LockUsage {
        val seed: Long
    }

    context(session: Session)
    fun lock(usage: LockUsage, name: String) {
        val query = queryOf(
            "select pg_advisory_xact_lock(hashtextextended(:name, :seed))",
            mapOf(
                "name" to name,
                "seed" to usage.seed
            )
        ).asExecute

        session.run(query)
    }
}