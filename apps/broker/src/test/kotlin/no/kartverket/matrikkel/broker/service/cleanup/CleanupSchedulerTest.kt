package no.kartverket.matrikkel.broker.service.cleanup

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThanOrEqualTo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class CleanupSchedulerTest {
    private fun topic(name: String, vararg policies: CleanupPolicy) = Topic(
        name = name,
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        cleanupPolicies = policies.toSet(),
    )

    private class RecordingService(val failOn: String? = null) : Cleanup.Service {
        val calls = CopyOnWriteArrayList<String>()

        override suspend fun cleanup(topic: Topic): Cleanup.Result {
            calls += topic.name
            if (topic.name == failOn) error("boom")
            return Cleanup.Result(topic.name, 0, 0, 0L)
        }
    }

    private fun runScheduler(topics: List<Topic>, service: RecordingService, until: () -> Boolean) = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val job = CleanupScheduler(topics, service, interval = 20.milliseconds).start(scope)
        try {
            withTimeout(10.seconds) { while (!until()) delay(10) }
        } finally {
            job.cancelAndJoin()
            scope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        }
    }

    @Test
    fun `only topics with cleanup policies are cleaned repeatedly`() {
        val service = RecordingService()
        val withPolicy = topic("with-policy", CleanupPolicy.Compact())
        val withoutPolicy = topic("without-policy")

        runScheduler(listOf(withPolicy, withoutPolicy), service) { service.calls.size >= 3 }

        assertThat(service.calls.toSet()).isEqualTo(setOf("with-policy"))
    }

    @Test
    fun `exception in one topic does not stop the loop`() {
        val service = RecordingService(failOn = "failing")
        val failing = topic("failing", CleanupPolicy.Compact())
        val healthy = topic("healthy", CleanupPolicy.Delete(retention = kotlin.time.Duration.parse("2d")))

        runScheduler(listOf(failing, healthy), service) { service.calls.count { it == "healthy" } >= 3 }

        assertThat(service.calls.count { it == "failing" }).isGreaterThanOrEqualTo(2)
    }
}
