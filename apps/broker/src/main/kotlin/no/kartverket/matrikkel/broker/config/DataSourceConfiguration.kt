package no.kartverket.matrikkel.broker.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import javax.sql.DataSource

object DataSourceConfiguration {
    fun createDatasource(
        url: String,
        credential: Credential
    ): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = url
            username = credential.username
            password = credential.password
            maximumPoolSize = 10
        }
        return HikariDataSource(config)
    }

    fun migrate(config: DatabaseConfiguration) {
        createDatasource(
            config.jdbcUrl,
            config.adminCredential
        )
            .use {
                migrate(it)
            }
    }

    fun migrate(dataSource: DataSource) {
        flywayConfig(dataSource)
            .load()
            .migrate()
    }

    fun flywayConfig(dataSource: DataSource): FluentConfiguration {
        return Flyway
            .configure()
            .dataSource(dataSource)
            .baselineOnMigrate(true)
            .baselineVersion("0")
    }
}