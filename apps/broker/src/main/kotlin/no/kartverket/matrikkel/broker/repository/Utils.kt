package no.kartverket.matrikkel.broker.repository

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotliquery.Session
import kotliquery.TransactionalSession
import kotliquery.sessionOf
import kotliquery.using
import javax.sql.DataSource

suspend fun <A> DataSource.withSession(operation: suspend Session.(Session) -> A): A {
    return withContext(Dispatchers.IO) {
        using(sessionOf(this@withSession)) { session ->
            runBlocking {
                with(session) {
                    operation(session)
                }
            }
        }
    }
}

suspend fun <A> DataSource.withTransaction(operation: suspend TransactionalSession.(TransactionalSession) -> A): A {
    return withContext(Dispatchers.IO) {
        using(sessionOf(this@withTransaction)) { session ->
            session.transaction {
                runBlocking(Dispatchers.IO) {
                    with(it) {
                        operation(it)
                    }
                }
            }
        }
    }
}