package no.kartverket.matrikkel.broker.plugins

import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

object Metrics {
    val registry: PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    class Config {
        var contextPath: String = ""
        var metricName: String = "ktor.http.server.requests"
        var meterBinders: List<MeterBinder> =
            listOf(
                ClassLoaderMetrics(),
                JvmMemoryMetrics(),
                JvmGcMetrics(),
                ProcessorMetrics(),
                JvmThreadMetrics(),
                FileDescriptorMetrics(),
            )
        var distributionStatisticConfig: DistributionStatisticConfig =
            DistributionStatisticConfig.Builder().percentiles(0.5, 0.9, 0.95, 0.99).build()

        internal var timerBuilder: Timer.Builder.(ApplicationCall, Throwable?) -> Unit = { _, _ -> }

        fun timers(block: Timer.Builder.(ApplicationCall, Throwable?) -> Unit) {
            timerBuilder = block
        }
    }

    val Plugin =
        createApplicationPlugin("Metrics", ::Config) {
            val config = pluginConfig
            application.install(MicrometerMetrics) {
                this.registry = Metrics.registry
                this.metricName = config.metricName
                this.meterBinders = config.meterBinders
                this.distributionStatisticConfig = config.distributionStatisticConfig
                this.timers(block = config.timerBuilder)
            }

            application.routing {
                route(config.contextPath) {
                    route("internal") {
                        get("metrics") {
                            call.respondText(registry.scrape())
                        }
                    }
                }
            }
        }
}