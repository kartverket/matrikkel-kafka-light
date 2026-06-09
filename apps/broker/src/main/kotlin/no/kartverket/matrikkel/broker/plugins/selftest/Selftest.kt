package no.kartverket.matrikkel.broker.plugins.selftest

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

object Selftest {
    class Config(
        appName: String = "Not set",
        version: String = "Not set",
        var contextPath: String = "",
    ) : SelftestGenerator.Config(appName, version)

    val Plugin =
        createApplicationPlugin("MatrikkelSelftest", ::Config) {
            val config = pluginConfig
            val selftest = SelftestGenerator.getInstance(config)

            application.routing {
                route(config.contextPath) {
                    route("internal") {
                        get("isAlive") {
                            if (selftest.isAlive()) {
                                call.respondText("Alive")
                            } else {
                                call.respondText("Not alive", status = HttpStatusCode.InternalServerError)
                            }
                        }

                        get("isReady") {
                            if (selftest.isReady()) {
                                call.respondText("Ready")
                            } else {
                                call.respondText("Not ready", status = HttpStatusCode.InternalServerError)
                            }
                        }

                        get("selftest") {
                            call.respondText(selftest.scrape())
                        }
                    }
                }
            }
        }
}