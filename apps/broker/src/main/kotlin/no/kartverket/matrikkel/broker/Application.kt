package no.kartverket.matrikkel.broker

import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.config.Configuration
import no.kartverket.matrikkel.broker.config.DataSourceConfiguration

fun runApplication() {
//    val config = Configuration()
//    DataSourceConfiguration.migrate(config.database)

    KtorServer.create(factory = Netty, port = 8081){
        //konfigurer her
        configureRouting()

    }.start(wait = true)
}