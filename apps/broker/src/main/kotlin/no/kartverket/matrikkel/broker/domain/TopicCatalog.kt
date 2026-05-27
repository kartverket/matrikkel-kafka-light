package no.kartverket.matrikkel.broker.domain
import kotlin.time.Duration.Companion.minutes

class TopicCatalog (
    topics: Collection<Topic> = defaultTopics(),
) {
    private val topicsByKey: Map<TopicKey, Topic> =
        topics.associateBy { it.key }

    init {
        require(topicsByKey.size == topics.size) {
            "Duplicate topic keys are not allowed"
        }

        require(TopicKey.entries.all { it in topicsByKey.keys }) {
            "All TopicKey values must be initialized. Missing: ${
                TopicKey.entries.filterNot { it in topicsByKey.keys }
            }"
        }
    }

    fun get(key: TopicKey): Topic =
        topicsByKey[key] ?: error("Unknown topic: $key")

    fun all(): Collection<Topic> =
        topicsByKey.values

    companion object {
        fun defaultTopics(): List<Topic> =
            listOf(
                Topic(
                    key = TopicKey.DEFAULT_TOPIC,
                    leaseTime = 5.minutes,
                    tombstonesAllowed = false,
                    acl = TopicAccessControlList(
                        publishIdentities = setOf(TopicAccessControlList.WILDCARD),
                        consumeIdentities = setOf(TopicAccessControlList.WILDCARD),
                    ),
                ),
            )
    }
}