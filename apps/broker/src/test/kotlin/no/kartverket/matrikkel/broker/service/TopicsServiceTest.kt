package no.kartverket.no.kartverket.matrikkel.broker.service

import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepository.TopicsDTO
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepositoryImpl
import no.kartverket.matrikkel.broker.service.TopicsServiceImpl
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TopicsServiceTest : WithDatabase{
    val repository = TopicsRepositoryImpl(dataSource())
    val messageService = TopicsServiceImpl(repository)
    val acl = TopicAccessControlList(
        publishIdentities = setOf("*"),
        consumeIdentities = setOf("*"),
    )

    @Test
    fun `Ny topic catalog oppdaters i db`() {
        runBlocking {
            messageService.reconcileTopics(listOf(Topic("test", acl)))

            val data = repository.getTopics()
            assertEquals(1, data.size)
            assertEquals(true, data.first().active)
        }
    }

    @Test
    fun `Gamle utdaterte topics deaktivers ved reconcile`() = runBlocking {
        repository.addTopic("test")

        messageService.reconcileTopics(listOf())

        val data = repository.getTopics()
        val map : Map<String, TopicsDTO> = data.associateBy { it.topic }
        assertEquals(1, data.size)
        assertEquals(false, map["test"]?.active)
    }

    @Test
    fun `Gamle topics kan reaktiveres ved reconcile`() = runBlocking {
        repository.addTopic("DEFAULT_TOPIC")
        repository.updateActive("DEFAULT_TOPIC", false)
        var data = repository.getTopics()
        assertEquals(1, data.size)
        assertEquals(false, data.first().active)

        messageService.reconcileTopics(
            listOf(Topic("DEFAULT_TOPIC", acl))
        )

        data = repository.getTopics()
        assertEquals(1, data.size)
        assertEquals(true, data.first().active)
    }

}