package no.kartverket.matrikkel.broker.api

import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.request.receive
import io.ktor.server.response.*
import io.ktor.server.routing.*
import no.kartverket.matrikkel.broker.ServiceException
import no.kartverket.matrikkel.broker.domain.ServiceIdentity
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.service.Messages
import no.kartverket.matrikkel.broker.utils.SealedResult

fun Route.topicRoutes(
    topicCatalog: TopicCatalog,
    messageService: Messages.Service,
) {
    with(topicCatalog) {
        route("/topics/{topic}") {
            post("publish") {
                // TODO VALIDATE HERE? Or in Messages.Service?
                call.respondResult(
                    messageService.publish(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("poll") {
                call.respondResult(
                    messageService.poll(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("commit") {
                call.respondResult(
                    messageService.commit(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("seek") {
                call.respondResult(
                    messageService.seek(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("heartbeat") {
                call.respondResult(
                    messageService.heartbeat(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }
        }
    }
}
private suspend fun <T : Any> ApplicationCall.respondResult(result: SealedResult<T>) {
    when (result) {
        is SealedResult.Success<*> -> respond(result.value)
        is SealedResult.Failure<*> -> throw result.error
    }
}

context(topicCatalog: TopicCatalog)
private fun ApplicationCall.topicParam(): Topic {
    val topicName = parameters["topic"]
    if (topicName.isNullOrBlank()) {
        throw ServiceException.badRequest("missing_topic", "Missing topic path parameter")
    }
    val topic = topicCatalog.getOrNull(topicName)
        ?: throw ServiceException.badRequest("missing_topic", "Topic configuration not found")

    return topic
}

private fun ApplicationCall.serviceIdentity(): ServiceIdentity {
    val subject = principal<JWTPrincipal>()?.subject
    if (subject.isNullOrBlank()) {
        throw ServiceException.unauthorized("unauthorized", "Missing topic path parameter")
    }
    return ServiceIdentity(subject)
}