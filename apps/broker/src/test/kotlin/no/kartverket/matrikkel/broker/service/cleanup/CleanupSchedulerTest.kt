package no.kartverket.matrikkel.broker.service.cleanup

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import no.kartverket.matrikkel.broker.domain.CleanupPolicy
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.domain.TopicAccessControlList
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds

class CleanupSchedulerTest {
    private fun topic(name: String, vararg policies: CleanupPolicy) = Topic(
        name = name,
        acl = TopicAccessControlList(setOf("*"), setOf("*")),
        cleanupPolicies = policies.toSet(),
    )

    private class FakeCleanupService(private val failOn: String? = null) : Cleanup.Service {
        val calls = mutableListOf<String>()

        override suspend fun cleanup(topic: Topic): Cleanup.Result {
            calls += topic.name
            if (topic.name == failOn) error("boom")
            return Cleanup.Result(topic.name, 0, 0, 0L)
        }
    }

    @Test
    fun `only topics with cleanup policies are cleaned repeatedly`() = runTest {
        val service = FakeCleanupService()
        val topics = listOf(topic("with-policy", CleanupPolicy.Compact()), topic("without-policy"))

        CleanupScheduler(topics, service, interval = 20.milliseconds).start(backgroundScope)
        delay(50.milliseconds)

        assertThat(service.calls).isEqualTo(List(3) { "with-policy" })
    }

    @Test
    fun `exception in one topic does not stop the loop`() = runTest {
        val service = FakeCleanupService(failOn = "failing")
        val topics = listOf(
            topic("failing", CleanupPolicy.Compact()),
            topic("healthy", CleanupPolicy.Delete(retention = 2.days)),
        )

        CleanupScheduler(topics, service, interval = 20.milliseconds).start(backgroundScope)
        delay(50.milliseconds)

        assertThat(service.calls.count { it == "failing" }).isEqualTo(3)
        assertThat(service.calls.count { it == "healthy" }).isEqualTo(3)
    }
}
