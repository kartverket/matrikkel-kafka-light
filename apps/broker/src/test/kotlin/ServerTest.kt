package no.kartverket
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import no.kartverket.matrikkel.broker.configureTopicRouting
import no.kartverket.matrikkel.broker.topic.TopicRegistry
import kotlin.test.*

class ServerTest {

    @Test
    fun `test root endpoint`() = testApplication {
        // loads default configuration
        configure()
        // verify server root returns 200
        assertEquals(HttpStatusCode.OK, client.get("/").status)
    }
    @Test
    fun `default topic is available`() = testApplication {
        application {
            configureTopicRouting(TopicRegistry())
        }

        assertEquals(HttpStatusCode.OK, client.get("/topics/default").status)
    }

}
