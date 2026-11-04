package no.kartverket.matrikkel.broker.domain

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class TopicCatalogTest {
    val testtopic = Topic(
        name = "DEFAULT_TOPIC",
        leaseTime = 30.seconds,
        tombstonesAllowed = true,
        acl = TopicAccessControlList(
            publishIdentities = setOf("producer-service"),
            consumeIdentities = setOf("consumer-service"),
        ),
    )

    @Test
    fun `topic acl allows wildcard publish and consume`() {
        val topic = testtopic.copy(
            acl = TopicAccessControlList(
                publishIdentities = setOf(TopicAccessControlList.WILDCARD),
                consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
            )
        )
        val identity = ServiceIdentity("et-eller-annet")

        assertTrue(topic.acl.canPublish(identity))
        assertTrue(topic.acl.canConsume(identity))
    }

    private fun rejects(vararg policies: CleanupPolicy) {
        assertFailsWith<IllegalArgumentException> {
            testtopic.copy(cleanupPolicies = policies.toSet())
        }
    }

    @Test
    fun `policies reject non-positive durations`() {
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Delete(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Compact(minCompactionLag = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CleanupPolicy.Compact(tombstoneRetention = (-1).hours) }
    }

    @Test
    fun `topic rejects more than one delete or compact policy`() {
        rejects(CleanupPolicy.Delete(2.days), CleanupPolicy.Delete(3.days))
        rejects(CleanupPolicy.Compact(minCompactionLag = 1.days), CleanupPolicy.Compact(minCompactionLag = 2.days))
    }

    @Test
    fun `topic accepts delete and compact together`() {
        val policies = setOf(CleanupPolicy.Delete(2.days), CleanupPolicy.Compact())

        val topic = testtopic.copy(cleanupPolicies = policies)

        assertEquals(policies, topic.cleanupPolicies)
    }

    @Test
    fun `can set up topic explicitly`() {
        val topicCatalog = TopicCatalog(listOf(testtopic))

        val topic = topicCatalog.get(testtopic.name)

        assertTrue(topic.acl.canPublish(ServiceIdentity("producer-service")))
        assertFalse(topic.acl.canPublish(ServiceIdentity("wrong-service")))
        assertTrue(topic.acl.canConsume(ServiceIdentity("consumer-service")))
    }
}