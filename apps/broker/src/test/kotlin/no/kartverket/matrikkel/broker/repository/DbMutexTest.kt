package no.kartverket.no.kartverket.matrikkel.broker.repository

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.kartverket.matrikkel.broker.repository.DbMutex
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase.Companion.dataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.uuid.Uuid

class DbMutexTest : WithDatabase {

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup() {
            sessionOf(dataSource()).use { session ->
                val query = queryOf("""
                    create table if not exists testtable(
                        sequence bigint not null primary key,
                        value text not null
                    )
                """.trimIndent())
                session.run(query.asExecute)
            }
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            sessionOf(dataSource()).use { session ->
                val query = queryOf("drop table testtable")
                session.run(query.asExecute)
            }
        }
    }

    @AfterEach
    fun cleanup() {
        sessionOf(dataSource()).use { session ->
            val query = queryOf("truncate testtable").asExecute
            session.run(query)
        }
    }

    @Test
    fun `should insert everything without issues when using locks`() {
        insertInParallell(locking = true)
        assertThat(getAllIds()).isEqualTo((1L..1000L).toList())
    }

    @Test
    fun `should fail if trying without locks`() {
        assertThrows<Exception> {
            insertInParallell(locking = false)
        }
    }

    private fun insertInParallell(locking: Boolean = true) {
        runBlocking {
            repeat(10) { w ->
                launch(Dispatchers.IO) {
                    repeat(100) { i ->
                        insertNext(locking)
                    }
                }
            }
        }
    }

    private fun getAllIds(): List<Long> {
        return sessionOf(dataSource()).use { session ->
            val query = queryOf("select sequence from testtable order by sequence asc")
                .map { it.long("sequence") }
                .asList

            session.run(query)
        }
    }

    object Lock : DbMutex.LockUsage {
        override val seed: Long = 123L
    }
    fun insertNext(doLocking: Boolean) {
        sessionOf(dataSource()).use { session ->
            session.transaction { tx ->
                with(tx) {
                    if (doLocking) {
                        DbMutex.lock(Lock, "same")
                    }
                    val max = getMax()
                    val nextValue = Uuid.random().toHexDashString()
                    insertValue(max + 1, nextValue)
                }
            }
        }
    }

    context(session: Session)
    fun getMax(): Long {
        val query = queryOf("select sequence from testtable order by testtable desc limit 1")
            .map { it.long("sequence") }
            .asSingle

        return session.run(query) ?: 0
    }

    context(session: Session)
    fun insertValue(sequence: Long, value: String) {
        val query = queryOf(
            "insert into testtable values(?, ?)",
            sequence, value
        ).asUpdate

        session.run(query)
    }
}