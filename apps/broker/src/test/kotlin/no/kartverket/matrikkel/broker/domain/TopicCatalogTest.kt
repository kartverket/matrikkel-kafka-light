package no.kartverket.matrikkel.broker.domain

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class TopicCatalogTest {
    @Test
    fun `default registry contains default topic`() {
        val registry = TopicCatalog()

        val topic = registry.get(TopicKey.DEFAULT_TOPIC)

        assertEquals(TopicKey.DEFAULT_TOPIC, topic.key)
        assertEquals(5.minutes, topic.leaseTime)
        assertFalse(topic.tombstonesAllowed)
    }

    @Test
    fun `topic acl allows wildcard publish and consume`() {
        val topic = TopicCatalog().get(TopicKey.DEFAULT_TOPIC)
        val identity = ServiceIdentity("et-eller-annet")

        assertTrue(topic.acl.canPublish(identity))
        assertTrue(topic.acl.canConsume(identity))
    }

    @Test
    fun `can set up topic explicitly`() {
        val topic = Topic(
            key = TopicKey.DEFAULT_TOPIC,
            leaseTime = 30.seconds,
            tombstonesAllowed = true,
            acl = TopicAccessControlList(
                publishIdentities = setOf("producer-service"),
                consumeIdentities = setOf("consumer-service"),
            ),
        )

        val registry = TopicCatalog(listOf(topic))

        assertTrue(registry.get(TopicKey.DEFAULT_TOPIC).acl.canPublish(ServiceIdentity("producer-service")))
        assertFalse(registry.get(TopicKey.DEFAULT_TOPIC).acl.canPublish(ServiceIdentity("wrong-service")))
        assertTrue(registry.get(TopicKey.DEFAULT_TOPIC).acl.canConsume(ServiceIdentity("consumer-service")))
    }
}