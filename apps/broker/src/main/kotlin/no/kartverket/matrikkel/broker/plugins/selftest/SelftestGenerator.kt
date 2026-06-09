package no.kartverket.matrikkel.broker.plugins.selftest

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking

class SelftestGenerator private constructor(
    private var config: Config,
) {
    companion object {
        private var instance = SelftestGenerator(Config())

        fun getInstance(config: Config): SelftestGenerator {
            instance.config = config
            return instance
        }
    }

    open class Config(
        var appName: String = "Not set",
        var version: String = "Not set",
    )

    sealed class Event(
        val reporter: Reporter,
    )

    class InitEvent(reporter: Reporter) : Event(reporter)

    class OkEvent(reporter: Reporter) : Event(reporter)

    class ErrorEvent(
        reporter: Reporter,
        val error: Throwable,
    ) : Event(reporter)

    private val statusMap = mutableMapOf<String, Event>()
    private val metadataMap = mutableMapOf<String, Metadata>()

    internal fun register(event: Event) {
        statusMap[event.reporter.name] = event
    }

    internal fun register(metadata: Metadata) {
        metadataMap[metadata.name] = metadata
    }

    fun isAlive(): Boolean = statusMap.values.none { it is ErrorEvent && it.reporter.critical }

    fun isReady(): Boolean {
        val initialized = statusMap.values.all { it !is InitEvent }
        val noCriticalError = isAlive()
        return initialized && noCriticalError
    }

    fun scrape(): String =
        buildString {
            appendLine("Appname: ${config.appName}")
            appendLine("Version: ${config.version}")
            appendLine()
            appendLine("Status:")
            for (result in statusMap.values) {
                val critical = if (result.reporter.critical) "(Critical)" else ""
                val status =
                    when (result) {
                        is InitEvent -> "Registered"
                        is OkEvent -> "OK"
                        is ErrorEvent -> "KO: ${result.error.message}"
                    }
                appendLine("\tName: ${result.reporter.name} $critical Status: $status")
            }

            if (metadataMap.isNotEmpty()) {
                appendLine()
                appendLine("Metadata:")
                for (metadata in metadataMap.values) {
                    appendLine("\tName: ${metadata.name} Value: ${metadata.value()}")
                }
            }
        }

    class Reporter(
        val name: String,
        val critical: Boolean,
    ) {
        init {
            instance.register(InitEvent(this))
        }

        fun reportOk() {
            instance.register(OkEvent(this))
        }

        fun reportError(error: Throwable) {
            instance.register(ErrorEvent(this, error))
        }

        fun ping(fn: suspend () -> Unit) {
            try {
                runBlocking(Dispatchers.IO) {
                    fn()
                }
                reportOk()
            } catch (error: Throwable) {
                reportError(error)
            }
        }
    }

    class Metadata(
        val name: String,
        val value: () -> String,
    ) {
        init {
            instance.register(this)
        }
    }
}