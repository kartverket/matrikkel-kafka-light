package no.kartverket.matrikkel.broker

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import no.kartverket.matrikkel.broker.topic.TopicKey
import no.kartverket.matrikkel.broker.topic.TopicRegistry

fun Application.configureRouting() {
    routing {
        get("/") {
            call.respondText("Hello, World!")
        }

        route("internal") {
            get("isAlive") {
                    call.respondText("Alive")
            }

            get("isReady") {
                    call.respondText("Ready")
            }

            get("selftest") {
                call.respondText("Helloworld")
            }
        }
    }
}

fun Application.configureTopicRouting(
    topicRegistry: TopicRegistry,
) {
    routing {
        get("/topics") {
            val topics = topicRegistry.all().joinToString(separator = "\n") {
                "${it.key.name}: ${it.key.displayName}"
            }

            call.respondText(topics)
        }

        get("/topics/default") {
            val topic = topicRegistry.get(TopicKey.DEFAULT_TOPIC)
            call.respondText("${topic.key.name}: ${topic.key.displayName}")
        }
    }
}