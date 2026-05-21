package no.kartverket.matrikkel.broker

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

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