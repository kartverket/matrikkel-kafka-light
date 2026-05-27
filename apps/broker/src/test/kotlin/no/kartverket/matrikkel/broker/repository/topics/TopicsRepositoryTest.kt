package no.kartverket.matrikkel.broker.repository.topics

import kotlinx.coroutines.runBlocking
import no.kartverket.no.kartverket.matrikkel.broker.testutils.WithDatabase
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TopicsRepositoryTest : WithDatabase {

    @Test
    fun `skal kunne legge til topic og hente ut`() = runBlocking {
        val repository = TopicsRepositoryImpl(dataSource())

        repository.addTopic("test")

        val data = repository.getTopics()

        assertEquals(1, data.size)
        assertEquals("test", data.first().topic)
        assertEquals(true, data.first().active)
    }

    @Test
    fun `skal kunne toggle om topic er aktiv`() = runBlocking {
        val repository = TopicsRepositoryImpl(dataSource())
        repository.addTopic("test")

        repository.updateActive("test", false)
        var data = repository.getTopics()
        assertEquals(false, data.first().active)

        repository.updateActive("test", true)
        data = repository.getTopics()
        assertEquals(true, data.first().active)
    }
}