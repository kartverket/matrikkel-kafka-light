package no.kartverket.no.kartverket.matrikkel.broker

import no.kartverket.matrikkel.broker.runApplication
import java.util.Properties
import kotlin.io.path.Path
import kotlin.io.path.reader

class Env {
    companion object {
        fun load(file: String) {
            val env = Properties()
            env.load(Path(file).reader())

            env.entries.forEach { entry ->
                System.setProperty(entry.key.toString(), entry.value.toString())
            }
        }
    }
}

fun main() {
    // Env.load("docker/local-postgres.env")
    Env.load("docker/local-h2.env")
    runApplication(disableSecurity = false)
}