package no.kartverket.matrikkel.broker.service

import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepository

class TopicsServiceImpl (val topicsRepository: TopicsRepository)  : TopicsService {
    override fun reconcileTopics(topics: List<Topic>) {
        val topicsInConfig = topics.associateBy { it.name }
        val topicsInDb = topicsRepository.getTopics().associateBy { it.topic }

        val activeTopicsInDb = topicsInDb.filter { it.value.active }
        val inactiveTopicsInDb = topicsInDb.filter { !it.value.active }

        // Legg til nye
        topicsInConfig.filter{it.key !in topicsInDb.keys }.forEach {
            topicsRepository.addTopic(it.key)
        }

        // Deaktiver fjernede
        activeTopicsInDb.filter { it.key !in topicsInConfig.keys }.forEach {
            topicsRepository.updateActive(it.value.topic, false)
        }

        // Reaktiver gamle
        inactiveTopicsInDb.filter { it.key in topicsInConfig.keys }.forEach {
            topicsRepository.updateActive(it.value.topic, true)
        }
    }
}