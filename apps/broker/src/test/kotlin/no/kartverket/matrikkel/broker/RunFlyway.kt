package no.kartverket.no.kartverket.matrikkel.broker

import no.kartverket.heimdall.common.kotlin.EnvUtils
import no.kartverket.heimdall.common.kotlin.EnvUtils.getConfig
import no.kartverket.matrikkel.broker.config.Credential
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.config.DatabaseConfiguration

fun main() {
    EnvUtils.load("docker/local-postgres.env")

    val database = DatabaseConfiguration(
        jdbcUrl = getConfig("DB_URL"),
        userCredential = Credential.from("DB_USER"),
        adminCredential = Credential.from("DB_ADMIN"),
    )
    DataSourceConfiguration.migrate(database)
}