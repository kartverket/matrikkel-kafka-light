package no.kartverket.no.kartverket.matrikkel.broker.testutils

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

interface WithDatabase {
    companion object {
        private val postgres = PostgreSQLContainer("postgres:18.3-alpine")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .apply { start() }

        private val dataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = postgres.jdbcUrl
                username = postgres.username
                password = postgres.password
                maximumPoolSize = 5
            }
        )


        fun dataSource(): DataSource = dataSource
        fun connectionUrl(): String = postgres.jdbcUrl
        private val flyway = DataSourceConfiguration.flywayConfig(dataSource)
            .cleanDisabled(false)
            .load()
    }

    @BeforeEach
    fun `migrate db`() {
        flyway.migrate()
    }

    @AfterEach
    fun `clean db`() {
        flyway.clean()
    }

    fun dataSource(): DataSource = dataSource
    fun connectionUrl(): String = postgres.jdbcUrl
}