package no.kartverket.matrikkel.broker.repository.topics

import kotliquery.Session
import java.time.Instant

interface TopicsRepository {
    data class TopicsDTO(
        val topic: String,
        val active: Boolean,
        val currentHead: Long,
        val updatedAt: Instant
    )

    fun getTopics(): Set<TopicsDTO>
    fun getTopics(session: Session): Set<TopicsDTO>
    fun addTopic(topic : String)
    fun addTopic(session: Session, topic: String)
    fun updateActive(topic: String, active: Boolean)
    fun updateActive(session: Session, topic: String, active: Boolean)
}