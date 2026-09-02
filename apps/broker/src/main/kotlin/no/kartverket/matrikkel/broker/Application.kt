package no.kartverket.matrikkel.broker

import io.ktor.http.*
import io.ktor.serialization.kotlinx.cbor.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.http.content.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.plugins.calllogging.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.routing.*
import kotlinx.serialization.cbor.Cbor
import no.kartverket.heimdall.common.ktor.plugins.Metrics
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.heimdall.common.ktor.plugins.selftest.Selftest
import no.kartverket.heimdall.common.ktor.utils.KtorServer
import no.kartverket.matrikkel.broker.api.topicRoutes
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.utils.TopicMetricsBinder
import org.slf4j.LoggerFactory
import kotlin.uuid.Uuid

fun runApplication(disableSecurity: Boolean = false) {
    val config = Configuration()
    val security = Security(
        config.azuread
    )
    DataSourceConfiguration.migrate(config.database)
    val dataSource = DataSourceConfiguration.createDatasource(
        config.database.jdbcUrl,
        config.database.userCredential,
    )

    KtorServer.create(factory = Netty, port = 8081) {
        standardPlugins(config.version)

        install(Authentication) {
            if (disableSecurity) {
                security.setupMock()
            } else {
                security.setupAuth()
            }
        }

        install(Metrics.Plugin) {
            meterBinders += TopicMetricsBinder(
                topicCatalog = config.topicsCatalog,
                dataSource = dataSource,
            )
        }

        routing {
            staticResources("/internal/introspect", "static")
            authenticate(*security.authproviders) {
                topicRoutes(
                    topicCatalog = config.topicsCatalog,
                    recordsService = Records.ServiceImpl(dataSource),
                )
            }
        }
    }.start(wait = true)
}

fun Application.standardPlugins(version: String) {
    install(ContentNegotiation) {
        cbor(
            Cbor {
                ignoreUnknownKeys = true
                encodeDefaults = true
            }
        )
    }

    install(StatusPages) {
        configureExceptionHandling()
    }

    install(CallId) {
        header(HttpHeaders.XRequestId)
        generate { Uuid.random().toString() }
    }

    install(CallLogging) {
        logger = LoggerFactory.getLogger("kafka_light")
        disableDefaultColors()
        filter { call -> call.request.path().contains("/internal/").not() }
        mdc("RequestId") { it.callId }
        mdc("CorrelationId") {
            it.request.header(HttpHeaders.XCorrelationId)
        }
        mdc("UserId") {
            it.principal<JWTPrincipal>()?.subject ?: "Anonymous"
        }
    }

    install(Selftest.Plugin) {
        this.appname = "matrikkel-kafka-light"
        this.version = version
    }
}