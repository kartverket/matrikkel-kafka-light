package no.kartverket.matrikkel.broker.domain

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

data class Topic(
    val name: String,
    val acl: TopicAccessControlList,
    val leaseTime: Duration = 30.seconds,
    val tombstonesAllowed: Boolean = true,
    val cleanupPolicies: Set<CleanupPolicy> = emptySet(),
    val minCleanableAge: Duration = 24.hours,
) {
    init {
        require(TopicName.isValid(name)) { "Invalid topic name '$name'" }
        require(minCleanableAge > Duration.ZERO) { "minCleanableAge must be positive" }
        val deletes = cleanupPolicies.filterIsInstance<CleanupPolicy.Delete>()
        val compacts = cleanupPolicies.filterIsInstance<CleanupPolicy.Compact>()
        require(deletes.size <= 1) { "At most one Delete policy is allowed" }
        require(compacts.size <= 1) { "At most one Compact policy is allowed" }
        deletes.singleOrNull()?.let {
            require(it.retention >= minCleanableAge) { "Delete retention must be >= minCleanableAge" }
        }
        compacts.singleOrNull()?.let {
            require(it.tombstoneRetention > Duration.ZERO) { "Compact tombstoneRetention must be positive" }
        }
    }
}

@JvmInline
value class ServiceIdentity(val value: String)

data class TopicAccessControlList(
    val publishIdentities: Set<String>,
    val consumeIdentities: Set<String>,
) {
    companion object {
        const val WILDCARD = "*"
    }
    fun canPublish(identity: ServiceIdentity): Boolean = publishIdentities.allows(identity)
    fun canConsume(identity: ServiceIdentity): Boolean = consumeIdentities.allows(identity)
    private fun Set<String>.allows(identity: ServiceIdentity): Boolean = WILDCARD in this || identity.value in this
}

object TopicName {
    private val pattern = Regex("^[a-zA-Z0-9._-]{1,128}$")

    fun isValid(value: String): Boolean = pattern.matches(value)
}