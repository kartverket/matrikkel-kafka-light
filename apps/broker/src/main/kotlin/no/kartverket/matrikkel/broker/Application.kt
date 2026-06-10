package no.kartverket.matrikkel.broker

import io.ktor.server.application.*
import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.plugins.Metrics
import no.kartverket.matrikkel.broker.plugins.Security
import no.kartverket.matrikkel.broker.plugins.selftest.Selftest

fun runApplication(disableSecurity: Boolean = false) {
    val config = Configuration()
    DataSourceConfiguration.migrate(config.database)

    KtorServer.create(factory = Netty, port = 8081) {
        install(Security.Plugin) {
            this.disableSecurity = disableSecurity
            providers += config.azuread
        }
        install(Metrics.Plugin)
        install(Selftest.Plugin) {
            appName = "matrikkel-kafka-light"
            version = config.version
        }
        configureRouting()
    }.start(wait = true)
}