package no.kartverket.matrikkel.broker.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
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
                migrate(it, config.env)
            }
    }

    fun migrate(dataSource: DataSource, env: MigrationEnv) = migrate(dataSource, *env.location)
    fun migrate(
        dataSource: DataSource,
        vararg locations: String,
    ) {
        Flyway
            .configure()
            .dataSource(dataSource)
            .locations(*locations)
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load()
            .migrate()
    }
}