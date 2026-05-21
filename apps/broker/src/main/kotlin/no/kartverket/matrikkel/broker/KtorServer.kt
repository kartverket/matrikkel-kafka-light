package no.kartverket.matrikkel.broker

import io.ktor.server.application.*
import io.ktor.server.engine.*
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.seconds


object KtorServer {

    private val logger = LoggerFactory.getLogger(KtorServer::class.java)

    fun <TEngine : ApplicationEngine, TConfiguration : ApplicationEngine.Configuration> create(
        factory: ApplicationEngineFactory<TEngine, TConfiguration>,
        port: Int = 8080,
        configure: TConfiguration.() -> Unit = {},
        application: Application.() -> Unit,
    ): EmbeddedServer<TEngine, TConfiguration> {
        val server =
            embeddedServer(
                factory,
                serverConfig(applicationEnvironment()) {
                    module {
                        application()
                    }
                },
                configure = {
                    connectors.add(
                        EngineConnectorBuilder().apply {
                            this.port = port
                        },
                    )
                    shutdownGracePeriod = 5.seconds.inWholeMilliseconds
                    shutdownTimeout = 30.seconds.inWholeMilliseconds
                    configure()
                },
            )

        server.addShutdownHook {
            logger.info("Shutdown hook called, shutting down gracefully")
        }

        return server
    }
}
