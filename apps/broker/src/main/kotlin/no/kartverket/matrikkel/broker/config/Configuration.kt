package no.kartverket.matrikkel.broker.config

import io.ktor.http.HttpHeaders
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.plugins.Security
import kotlin.time.Duration.Companion.minutes

class DatabaseConfiguration(
    val jdbcUrl: String,
    val userCredential: Credential,
    val adminCredential: Credential,
)

class Configuration(
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

private fun getConfig(name: String): String? {
    return System.getProperty(name, System.getenv(name))
}

private fun getRequiredConfig(name: String): String {
    return requireNotNull(getConfig(name)) {
        "$name must be defined in java properties or environment"
    }
}

private fun firstNonNullOf(vararg name: String): String {
    return name.firstNotNullOf { getConfig(it) }
}