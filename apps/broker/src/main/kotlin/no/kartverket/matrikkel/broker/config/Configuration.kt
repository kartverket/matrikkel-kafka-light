package no.kartverket.matrikkel.broker.config

import io.ktor.http.HttpHeaders
import io.ktor.server.auth.Credential
import no.kartverket.heimdall.common.kotlin.EnvUtils.getConfig
import no.kartverket.heimdall.common.kotlin.EnvUtils.getConfigOrNull
import no.kartverket.heimdall.common.ktor.plugins.security.Security
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
    val version: String = getConfig("VERSION"),
    val matrikkelEksternDataIngestorIdentity: String = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_EKSTERN_DATA_INGESTOR"),
    val matrikkelSergSyncIdentity: String = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_SERG_SYNC"),
    val azuread: Security.AuthProvider = Security.AuthProvider(
        name = "azuread",
        jwksConfig = Security.JwksConfig.OidcWellkownUrl(
            getConfig("AZURE_APP_WELL_KNOWN_URL")
        ),
        tokenLocation = Security.TokenLocation.Header(HttpHeaders.Authorization)
    ),
    val database: DatabaseConfiguration = DatabaseConfiguration(
        jdbcUrl = getConfig("DB_URL"),
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
            Topic(
                name = "SERG_HENDELSER",
                leaseTime = 5.minutes,
                tombstonesAllowed = false,
                acl = TopicAccessControlList(
                    publishIdentities = setOf(matrikkelSergSyncIdentity),
                    consumeIdentities = setOf(matrikkelSergSyncIdentity),
                ),
            ),
            Topic(
                name = "SERG_FORMUESOBJEKT_FAST_EIENDOM",
                leaseTime = 5.minutes,
                tombstonesAllowed = false,
                acl = TopicAccessControlList(
                    publishIdentities = setOf(matrikkelSergSyncIdentity),
                    consumeIdentities = setOf(matrikkelEksternDataIngestorIdentity),
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
    return name.firstNotNullOf { getConfigOrNull(it) }
}