package no.kartverket.no.kartverket.matrikkel.broker

import no.kartverket.heimdall.common.ktor.utils.EnvUtils.getRequiredConfig
import no.kartverket.matrikkel.broker.config.Credential
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.config.DatabaseConfiguration

fun main() {
    Env.load("docker/local-postgres.env")

    val database = DatabaseConfiguration(
        jdbcUrl = getRequiredConfig("DB_URL"),
        userCredential = Credential.from("DB_USER"),
        adminCredential = Credential.from("DB_ADMIN"),
    )
    DataSourceConfiguration.migrate(database)
}