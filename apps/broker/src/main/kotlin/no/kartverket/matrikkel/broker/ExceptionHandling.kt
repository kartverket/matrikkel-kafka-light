package no.kartverket.matrikkel.broker

import io.ktor.http.*
import io.ktor.server.plugins.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import kotlinx.serialization.Serializable

fun StatusPagesConfig.configureExceptionHandling() {
    exception<ServiceException> { call, cause ->
        call.respond(cause.status, ErrorResponse(cause.code, cause.message))
    }

    exception<BadRequestException> { call, cause ->
        call.respond(
            HttpStatusCode.BadRequest,
            ErrorResponse("invalidRequest", cause.message ?: "The request is invalid"),
        )
    }

    exception<IllegalArgumentException> { call, cause ->
        call.respond(
            HttpStatusCode.BadRequest,
            ErrorResponse("invalidRequest", cause.message ?: "The request is invalid"),
        )
    }

    exception<Throwable> { call, _ ->
        call.respond(
            HttpStatusCode.InternalServerError,
            ErrorResponse("internal_error", "An internal error occurred"),
        )
    }
}

@Serializable
data class ErrorResponse(
    val code: String,
    val message: String,
)

class ServiceException(
    val status: HttpStatusCode,
    val code: String,
    override val message: String
) : RuntimeException(message) {
    companion object {
        fun badRequest(code: String, message: String) = ServiceException(
            status = HttpStatusCode.BadRequest,
            code = code,
            message = message
        )

        fun unauthorized(code: String, message: String) = ServiceException(
            status = HttpStatusCode.Unauthorized,
            code = code,
            message = message
        )
    }
}