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

    private fun rejects(minCleanableAge: Duration = 24.hours, vararg policies: CleanupPolicy) {
        assertFailsWith<IllegalArgumentException> {
            testtopic.copy(minCleanableAge = minCleanableAge, cleanupPolicies = policies.toSet())
        }
    }

    @Test
    fun `topic rejects non-positive minCleanableAge`() {
        rejects(minCleanableAge = Duration.ZERO)
        rejects(minCleanableAge = (-1).hours)
    }

    @Test
    fun `topic rejects more than one delete or compact policy`() {
        rejects(policies = arrayOf(CleanupPolicy.Delete(2.days), CleanupPolicy.Delete(3.days)))
        rejects(policies = arrayOf(CleanupPolicy.Compact(1.days), CleanupPolicy.Compact(2.days)))
    }

    @Test
    fun `topic rejects delete retention shorter than minCleanableAge`() {
        rejects(minCleanableAge = 2.days, policies = arrayOf(CleanupPolicy.Delete(1.days)))
    }

    @Test
    fun `topic rejects non-positive tombstoneRetention`() {
        rejects(policies = arrayOf(CleanupPolicy.Compact(Duration.ZERO)))
    }

    @Test
    fun `topic accepts delete retention equal to minCleanableAge and both policies together`() {
        val policies = setOf(CleanupPolicy.Delete(2.days), CleanupPolicy.Compact())

        val topic = testtopic.copy(minCleanableAge = 2.days, cleanupPolicies = policies)

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