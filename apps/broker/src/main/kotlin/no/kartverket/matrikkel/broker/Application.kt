package no.kartverket.matrikkel.broker

import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.*
import io.ktor.server.auth.Authentication
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.*
import kotlinx.serialization.json.Json
import no.kartverket.heimdall.common.ktor.plugins.Metrics
import no.kartverket.heimdall.common.ktor.plugins.selftest.Selftest
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.heimdall.common.ktor.utils.KtorServer
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration

fun runApplication(disableSecurity: Boolean = false) {
    val config = Configuration()
    val security = Security(
        config.azuread
    )
    DataSourceConfiguration.migrate(config.database)

    KtorServer.create(factory = Netty, port = 8081) {
        install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true
                    encodeDefaults = true
                }
            )
        }

        install(StatusPages) {
            configureExceptionHandling()
        }

        install(Authentication) {
            if (disableSecurity) {
                security.setupMock()
            } else {
                security.setupAuth()
            }
        }
        install(Metrics.Plugin)
        install(Selftest.Plugin) {
            appname = "matrikkel-kafka-light"
            version = config.version
        }
    }.start(wait = true)
}