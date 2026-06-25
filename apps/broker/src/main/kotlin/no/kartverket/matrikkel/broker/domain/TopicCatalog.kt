package no.kartverket.matrikkel.broker.domain

class TopicCatalog(
    val topics: List<Topic>,
) {
    private val topicsByKey: Map<String, Topic> = topics.associateBy { it.name }

    init {
        require(topicsByKey.size == topics.size) {
            "Duplicate topic keys are not allowed"
        }
    }

    fun get(key: String): Topic = requireNotNull(topicsByKey[key]) {
        "Unknown topic: $key"
    }
    fun getOrNull(key: String): Topic? = topicsByKey[key]

    fun all(): Collection<Topic> = topicsByKey.values
}