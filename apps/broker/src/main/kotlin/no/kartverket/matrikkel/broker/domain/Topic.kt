package no.kartverket.matrikkel.broker.domain

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class Topic(
    val name: String,
    val acl: TopicAccessControlList,
    val leaseTime: Duration = 30.seconds,
    val tombstonesAllowed: Boolean = true,
) {
    init {
        // TODO validate
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