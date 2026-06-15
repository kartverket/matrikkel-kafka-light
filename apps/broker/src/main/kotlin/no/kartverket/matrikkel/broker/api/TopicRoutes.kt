package no.kartverket.matrikkel.broker.api

import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import no.kartverket.matrikkel.broker.ServiceException
import no.kartverket.matrikkel.broker.domain.ServiceIdentity

fun Route.topicRoutes() {
    route("/topics/{topic}") {
        post("publish") {
            val topic = call.topicParam()
            val identity = call.serviceIdentity()
            call.respondText("publish to $topic by $identity")
        }
        post("poll") {
            val topic = call.topicParam()
            val identity = call.serviceIdentity()
            call.respondText("poll to $topic by $identity")
        }
        post("commit") {
            val topic = call.topicParam()
            val identity = call.serviceIdentity()
            call.respondText("commit to $topic by $identity")
        }
        post("seek") {
            val topic = call.topicParam()
            val identity = call.serviceIdentity()
            call.respondText("seek to $topic by $identity")
        }
        post("heartbeat") {
            val topic = call.topicParam()
            val identity = call.serviceIdentity()
            call.respondText("heartbeat to $topic by $identity")
        }
    }
}

private fun ApplicationCall.topicParam(): String {
    val topic = parameters["topic"]
    if (topic.isNullOrBlank()) {
        throw ServiceException.badRequest("missing_topic", "Missing topic path parameter")
    }
    return topic
}

private fun ApplicationCall.serviceIdentity(): ServiceIdentity {
    val subject = principal<JWTPrincipal>()?.subject
    if (subject.isNullOrBlank()) {
        throw ServiceException.unauthorized("unauthorized", "Missing topic path parameter")
    }
    return ServiceIdentity(subject)
}