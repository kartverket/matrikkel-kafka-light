package no.kartverket.matrikkel.broker.domain

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
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

    @Test
    fun `can set up topic explicitly`() {
        val topicCatalog = TopicCatalog(listOf(testtopic))

        val topic = topicCatalog.get(testtopic.name)

        assertTrue(topic.acl.canPublish(ServiceIdentity("producer-service")))
        assertFalse(topic.acl.canPublish(ServiceIdentity("wrong-service")))
        assertTrue(topic.acl.canConsume(ServiceIdentity("consumer-service")))
    }
}