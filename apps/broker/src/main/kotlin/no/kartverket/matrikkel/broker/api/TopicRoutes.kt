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
import no.kartverket.matrikkel.broker.service.records.Records
import no.kartverket.matrikkel.broker.utils.SealedResult
import no.kartverket.matrikkel.kafkaclient.PublishRequest

fun Route.topicRoutes(
    topicCatalog: TopicCatalog,
    recordsService: Records.Service,
) {
    with(topicCatalog) {
        route("/topics/{topic}") {
            post("publish") {
                val topic = call.topicParam()
                val identity = call.serviceIdentity()
                val request = call.receive<PublishRequest>()

                call.respondResult(
                    when {
                        !topic.acl.canPublish(identity) -> forbidden()
                        request.payload == null && !topic.tombstonesAllowed -> badRequest("tombstone_not_allow", "Payload cannot be null")
                        request.recordKey.isBlank() -> badRequest("invalid_request", "recordKey cannot be blank")
                        request.idempotencyKey.isBlank() -> badRequest("invalid_request", "idempotencyKey cannot be blank")
                        request.correlationId.isBlank() -> badRequest("invalid_request", "correlationId cannot be blank")
                        else -> {
                            recordsService.publish(
                                topic = topic,
                                identity = identity,
                                request = request,
                            )
                        }
                    }
                )
            }

            post("poll") {
                call.respondResult(
                    recordsService.poll(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("commit") {
                call.respondResult(
                    recordsService.commit(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("seek") {
                call.respondResult(
                    recordsService.seek(
                        topic = call.topicParam(),
                        identity = call.serviceIdentity(),
                        request = call.receive(),
                    )
                )
            }

            post("heartbeat") {
                call.respondResult(
                    recordsService.heartbeat(
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
        is SealedResult.Failure -> throw result.error
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

private fun forbidden(
    code: String = "forbidden",
    message: String = "Service identity not authorized to execute command on this topic"
) = ServiceException
    .forbidden(code = code, message = message)
    .asSealedResult()

private fun badRequest(code: String, message: String) = ServiceException
    .badRequest(code = code, message = message)
    .asSealedResult()