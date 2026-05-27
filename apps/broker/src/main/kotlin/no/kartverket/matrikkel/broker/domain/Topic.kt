package no.kartverket.matrikkel.broker.domain

import kotlin.time.Duration

data class Topic(
    val key: TopicKey,
    val leaseTime: Duration,
    val tombstonesAllowed: Boolean,
    val acl: TopicAccessControlList,
)

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
enum class TopicKey(val displayName: String) {
    DEFAULT_TOPIC("Default topic")
}