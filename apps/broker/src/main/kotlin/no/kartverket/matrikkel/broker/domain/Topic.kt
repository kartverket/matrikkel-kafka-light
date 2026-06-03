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
        require(TopicName.isValid(name)) { "Invalid topic name '$name'" }
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