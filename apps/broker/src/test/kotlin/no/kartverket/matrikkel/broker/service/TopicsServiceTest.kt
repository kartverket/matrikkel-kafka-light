package no.kartverket.no.kartverket.matrikkel.broker.service

import kotlinx.coroutines.runBlocking
import no.kartverket.matrikkel.broker.config.TopicConfiguration
import no.kartverket.matrikkel.broker.domain.TopicCatalog
import no.kartverket.matrikkel.broker.domain.TopicKey
import no.kartverket.matrikkel.broker.repository.topics.TopicsReadDTO
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepositoryImpl
import no.kartverket.matrikkel.broker.service.TopicsServiceImpl
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TopicsServiceTest : WithDatabase{
    val repository = TopicsRepositoryImpl(dataSource())
    val messageService = TopicsServiceImpl(repository)

    @Test
    fun `Ny topic catalog oppdaters i db`() = runBlocking {

        messageService.reconcileTopics(TopicConfiguration(TopicCatalog()))

        val data = repository.getTopics()
        assertEquals(1, data.size)
        assertEquals(true, data.first().active)
    }

    @Test
    fun `Gamle utdaterte topics deaktivers ved reconcile`() = runBlocking {

        repository.addTopic("test")

        messageService.reconcileTopics(TopicConfiguration(TopicCatalog()))

        val data = repository.getTopics()
        val map : Map<String, TopicsReadDTO> = data.associateBy { it.topic }
        assertEquals(2, data.size)
        assertEquals(false, map["test"]?.active)
    }

    @Test
    fun `Gamle topics kan reaktiveres ved reconcile`() = runBlocking {

        repository.addTopic(TopicKey.DEFAULT_TOPIC.name)
        repository.updateActive(TopicKey.DEFAULT_TOPIC.name, false)
        var data = repository.getTopics()
        assertEquals(1, data.size)
        assertEquals(false, data.first().active)

        messageService.reconcileTopics(TopicConfiguration(TopicCatalog()))

        data = repository.getTopics()
        assertEquals(1, data.size)
        assertEquals(true, data.first().active)
    }

}