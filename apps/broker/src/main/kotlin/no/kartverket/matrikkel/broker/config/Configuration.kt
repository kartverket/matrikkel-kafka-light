package no.kartverket.matrikkel.broker.config

import io.ktor.http.HttpHeaders
import no.kartverket.heimdall.common.kotlin.EnvUtils.getConfig
import no.kartverket.heimdall.common.kotlin.EnvUtils.getConfigOrNull
import no.kartverket.heimdall.common.ktor.plugins.security.Security
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes

class DatabaseConfiguration(
    val jdbcUrl: String,
    val userCredential: Credential,
    val adminCredential: Credential,
)

class CleanupSchedulerConfiguration(
    val interval: Duration,
    val batchSize: Int,
)

class Configuration(
    val version: String = getConfig("VERSION"),
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
    val cleanup: CleanupSchedulerConfiguration = CleanupSchedulerConfiguration(
        interval = 10.minutes,
        batchSize = 1000,
    ),
    val topicsCatalog: TopicCatalog = TopicCatalog(
        listOf(
            *sergTopics(),
            *fregTopics(),
            *brregTopics(),
        )
    )
)

fun sergTopics(): Array<Topic> {
    val sergSyncApp: String = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_SERG_SYNC")
    val ingestorApp: String = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_EKSTERN_DATA_INGESTOR")

    return  arrayOf(
        Topic(
            name = "SERG_HENDELSER",
            leaseTime = 5.minutes,
            tombstonesAllowed = false,
            acl = TopicAccessControlList.internalTopicFor(sergSyncApp),
        ),
        Topic(
            name = "SERG_FORMUESOBJEKT_FAST_EIENDOM",
            leaseTime = 5.minutes,
            tombstonesAllowed = false,
            acl = TopicAccessControlList(
                publishIdentities = setOf(sergSyncApp),
                consumeIdentities = setOf(ingestorApp),
            ),
        ),
    )
}

fun fregTopics(): Array<Topic> {
    return arrayOf()
}

fun brregTopics(): Array<Topic> {
    val brregSyncApp = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_BRREG_SYNC")
    val ingestorApp: String = getConfig("AZURE_AD_SERVICE_PRINCIPAL_MATRIKKEL_EKSTERN_DATA_INGESTOR")

    return arrayOf(
        Topic(
            name = "BRREG_OFFSET",
            acl = TopicAccessControlList.internalTopicFor(brregSyncApp),
            leaseTime = 2.minutes,
            tombstonesAllowed = false,
            compactionPolicy = CleanupPolicy.Compact(
                minCompactionLag = 5.minutes
            ),
        ),
        Topic(
            name = "BRREG_HENDELSER",
            acl = TopicAccessControlList.internalTopicFor(brregSyncApp),
            leaseTime = 2.minutes,
            tombstonesAllowed = false,
            deletionPolicy = CleanupPolicy.Delete(
                retention = 30.days
            ),
        ),
        Topic(
            name = "BRREG_VIRKSOMHET",
            acl = TopicAccessControlList(
                publishIdentities = setOf(brregSyncApp),
                consumeIdentities = setOf(ingestorApp),
            ),
            leaseTime = 2.minutes,
            tombstonesAllowed = true,
            compactionPolicy = CleanupPolicy.Compact(
                minCompactionLag = 5.minutes,
            ),
        ),
    )
}

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