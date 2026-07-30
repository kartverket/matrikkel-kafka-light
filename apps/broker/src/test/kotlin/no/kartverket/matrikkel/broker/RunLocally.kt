package no.kartverket.no.kartverket.matrikkel.broker

import no.kartverket.matrikkel.broker.runApplication

fun main() {
     Env.load("docker/local-postgres.env")
//    Env.load("docker/local-h2.env")
    runApplication(disableSecurity = true)
}