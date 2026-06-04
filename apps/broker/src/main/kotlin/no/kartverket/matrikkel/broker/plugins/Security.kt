package no.kartverket.matrikkel.broker.plugins

import com.auth0.jwk.JwkProvider
import com.auth0.jwk.JwkProviderBuilder
import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.http.auth.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.utils.OidcClient
import java.net.URI

object Security {
    class AuthProvider(
        val name: String,
        val jwksConfig: JwksConfig,
        val tokenLocation: TokenLocation,
    )

    sealed interface JwksConfig {
        val jwksUrl: String
        val issuer: String

        class JwksUrl(
            override val jwksUrl: String,
            override val issuer: String,
        ) : JwksConfig

        class OidcWellkownUrl(url: String) : JwksConfig {
            private val oidcClient = OidcClient(url)

            private val config: OidcClient.OidcDiscoveryConfig by lazy { runBlocking(Dispatchers.IO) { oidcClient.fetch() } }

            override val jwksUrl: String by lazy { config.jwksUrl }
            override val issuer: String by lazy { config.issuer }
        }
    }

    sealed interface TokenLocation {
        fun extract(call: ApplicationCall): String?

        class Cookie(val name: String) : TokenLocation {
            override fun extract(call: ApplicationCall): String? = call.request.cookies[name]
        }

        class Header(val name: String) : TokenLocation {
            override fun extract(call: ApplicationCall): String? = call.request.headers[name]
        }
    }

    class PluginConfig {
        var disableSecurity: Boolean = false
        val providers = mutableListOf<AuthProvider>()
    }

    val Plugin = createApplicationPlugin("Security", ::PluginConfig) {
        val config = pluginConfig
        with(application) {
            install(Authentication) {
                if (config.disableSecurity) {
                    setupMock(config)
                } else {
                    setupAuth(config)
                }
            }
        }
    }

    private fun AuthenticationConfig.setupAuth(config: PluginConfig) {
        for (authProvider in config.providers) {
            jwt(authProvider.name) {
                authHeader { call ->
                    val token = authProvider.tokenLocation.extract(call) ?: ""
                    parseAuthorizationHeader(token)
                }
                verifier(makeJwkVerifier(authProvider.jwksConfig.jwksUrl))
                validate { credentials ->
                    checkNotNull(credentials.payload.audience) { "Audience was not present in jwt" }
                    check(credentials.payload.issuer == authProvider.jwksConfig.issuer) {
                        "Issuer did not match provider config. Expected: '${authProvider.jwksConfig.issuer}', but got: '${credentials.payload.issuer}'"
                    }

                    JWTPrincipal(credentials.payload)
                }
            }
        }
    }

    private fun AuthenticationConfig.setupMock(config: PluginConfig) {
        val token = JWT.decode(JWT.create().withSubject("fake-user").sign(Algorithm.none()))
        for (authProvider in config.providers) {
            provider(authProvider.name) {
                authenticate {
                    it.principal(JWTPrincipal(token))
                }
            }
        }
    }

    private fun makeJwkVerifier(jwksUrl: String): JwkProvider {
        return JwkProviderBuilder(URI(jwksUrl).toURL())
            .cached(true)
            .rateLimited(true)
            .build()
    }
}