package no.kartverket.matrikkel.broker

import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration
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
    topicService.reconcileTopics(config.topicsCatalog.topics)

    KtorServer.create(factory = Netty, port = 8081){
        configureRouting()
        //konfigurer her
    }.start(wait = true)
}