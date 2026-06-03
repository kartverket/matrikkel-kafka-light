package no.kartverket.matrikkel.broker.service

import no.kartverket.matrikkel.broker.domain.Topic

interface TopicsService {
    fun reconcileTopics(topics: List<Topic>)
}