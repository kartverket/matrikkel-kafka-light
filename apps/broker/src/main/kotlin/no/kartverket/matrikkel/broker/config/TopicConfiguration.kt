package no.kartverket.matrikkel.broker.config

import no.kartverket.matrikkel.broker.domain.TopicCatalog

data class TopicConfiguration (
    val topicCatalog : TopicCatalog
)

fun Configuration.topicConfiguration () : TopicConfiguration {
    return TopicConfiguration(TopicCatalog())
}