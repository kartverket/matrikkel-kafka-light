package no.kartverket.matrikkel.broker

import io.ktor.server.netty.*
import no.kartverket.matrikkel.broker.topic.TopicRegistry

fun runApplication() {
    val topicRegistry = TopicRegistry()

    KtorServer.create(factory = Netty, port = 8081) {
        configureRouting()
        configureTopicRouting(topicRegistry)
    }.start(wait = true)
}