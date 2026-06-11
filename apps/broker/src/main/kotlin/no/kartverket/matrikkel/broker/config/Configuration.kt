package no.kartverket.matrikkel.broker.config

import io.ktor.http.HttpHeaders
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.heimdall.common.ktor.utils.EnvUtils.getConfig
import no.kartverket.heimdall.common.ktor.utils.EnvUtils.getRequiredConfig
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import kotlin.time.Duration.Companion.minutes

class DatabaseConfiguration(
    val jdbcUrl: String,
    val userCredential: Credential,
    val adminCredential: Credential,
)

class Configuration(
    val version: String = getRequiredConfig("VERSION"),
    val azuread: Security.AuthProvider = Security.AuthProvider(
        name = "azuread",
        jwksConfig = Security.JwksConfig.OidcWellkownUrl(
            getRequiredConfig("AZURE_APP_WELL_KNOWN_URL")
        ),
        tokenLocation = Security.TokenLocation.Header(HttpHeaders.Authorization)
    ),
    val database: DatabaseConfiguration = DatabaseConfiguration(
        jdbcUrl = getRequiredConfig("DB_URL"),
        userCredential = Credential.from("DB_USER"),
        adminCredential = Credential.from("DB_ADMIN"),
    ),
    val topicsCatalog: TopicCatalog = TopicCatalog(
        listOf(
            Topic(
                name = "DEFAULT_TOPIC",
                leaseTime = 5.minutes,
                tombstonesAllowed = false,
                acl = TopicAccessControlList(
                    publishIdentities = setOf(TopicAccessControlList.WILDCARD),
                    consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
                ),
            ),
        )
    )
)

class Credential(
    val username: String,
    val password: String,
) {
    companion object {
        fun from(name: String) = Credential(
            username = firstNonNullOf("${name}_USERNAME", "${name}_USER"),
            password = firstNonNullOf("${name}_PASSWORD"),
        )
    }
}


private fun firstNonNullOf(vararg name: String): String {
    return name.firstNotNullOf { getConfig(it) }
}