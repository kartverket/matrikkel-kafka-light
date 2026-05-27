package no.kartverket.matrikkel.broker.repository.topics

import kotliquery.Session
import no.kartverket.matrikkel.broker.repository.Repository

interface TopicsRepository : Repository {
    fun getTopics(): Set<TopicsReadDTO>
    fun getTopics(session: Session): Set<TopicsReadDTO>
    fun addTopic(topic : String)
    fun addTopic(topic : String, session: Session)
    fun updateActive(topic: String, active: Boolean)
    fun updateActive(topic: String, active: Boolean, session: Session)
}