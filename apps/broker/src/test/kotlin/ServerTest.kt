package no.kartverket

import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import no.kartverket.matrikkel.broker.configureRouting
import kotlin.test.*

class ServerTest {

    @Test
    fun `test root endpoint`() = testApplication {
        application {
            configureRouting()
        }
        assertEquals(HttpStatusCode.OK, client.get("/").status)
    }

}
