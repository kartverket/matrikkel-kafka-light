package no.kartverket.matrikkel.broker

import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
import no.kartverket.matrikkel.broker.config.topicConfiguration
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepositoryImpl
import no.kartverket.matrikkel.broker.service.TopicsServiceImpl

fun runApplication() {
    val config = Configuration()
    DataSourceConfiguration.migrate(config.database)

    val topicService = TopicsServiceImpl(
        TopicsRepositoryImpl(
            DataSourceConfiguration
                .createDatasource(config.database.jdbcUrl, config.database.adminCredential)
        )
    )
    topicService.reconcileTopics(config.topicConfiguration())

    KtorServer.create(factory = Netty, port = 8081){
        //konfigurer her
        configureRouting()

    }.start(wait = true)
}