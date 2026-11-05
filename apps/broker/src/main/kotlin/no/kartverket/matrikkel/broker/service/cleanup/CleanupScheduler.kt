package no.kartverket.matrikkel.broker.service.cleanup

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.kartverket.matrikkel.broker.domain.Topic
import org.slf4j.LoggerFactory
import kotlin.time.Duration

class CleanupScheduler(
    topics: Collection<Topic>,
    private val service: Cleanup.Service,
    private val interval: Duration,
    private val onCleanup: (Cleanup.Result) -> Unit = {},
) {
    private val log = LoggerFactory.getLogger(CleanupScheduler::class.java)
    private val topics = topics.filter { it.deletionPolicy != null || it.compactionPolicy != null }

    fun start(scope: CoroutineScope): Job = scope.launch {
        while (scope.isActive) {
            for (topic in topics) {
                try {
                    onCleanup(service.cleanup(topic))
                } catch (e: TimeoutCancellationException) {
                    log.warn("Timed out waiting for cleanup job", e)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Cleanup failed for topic {}", topic.name, e)
                }
            }
            delay(interval)
        }
    }
}
