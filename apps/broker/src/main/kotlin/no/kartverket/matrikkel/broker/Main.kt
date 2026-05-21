package no.kartverket.no.kartverket.matrikkel.broker

import io.ktor.server.netty.*

fun main() {
    KtorServer.create(factory = Netty, port = 8081){
        //konfigurer her
        configureRouting()

    }.start(wait = true)
}
