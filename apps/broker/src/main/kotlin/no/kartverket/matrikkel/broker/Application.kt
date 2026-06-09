package no.kartverket.matrikkel.broker

import io.ktor.server.application.*
import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.plugins.Metrics
import no.kartverket.matrikkel.broker.plugins.Security
import no.kartverket.matrikkel.broker.plugins.selftest.Selftest
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepositoryImpl
import no.kartverket.matrikkel.broker.service.TopicsServiceImpl

fun runApplication(disableSecurity: Boolean = false) {
    val config = Configuration()
    DataSourceConfiguration.migrate(config.database)

    val topicService = TopicsServiceImpl(
        TopicsRepositoryImpl(
            DataSourceConfiguration
                .createDatasource(config.database.jdbcUrl, config.database.adminCredential)
        )
    )
    topicService.reconcileTopics(config.topicsCatalog.topics)

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