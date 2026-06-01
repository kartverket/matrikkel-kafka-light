package no.kartverket.matrikkel.broker.repository.topics

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.kartverket.matrikkel.broker.repository.topics.TopicsRepository.TopicsDTO
import javax.sql.DataSource

class TopicsRepositoryImpl(val dataSource: DataSource) : TopicsRepository {

    override fun getTopics(): Set<TopicsDTO> {
        return sessionOf(dataSource).use { session ->
            getTopics(session)
        }
    }

    override fun getTopics(session: Session): Set<TopicsDTO> {
        return session.run(
            queryOf("SELECT * FROM topics")
                .map { row ->
                    TopicsDTO(
                        topic = row.string("topic"),
                        active = row.boolean("active"),
                        currentHead = row.long("current_head"),
                        updatedAt = row.instant("updated_at"),
                    )
                }.asList
        ).toSet()
    }

    override fun addTopic(topic: String) {
        sessionOf(dataSource).use { session ->
            addTopic(session, topic)
        }
    }

    override fun addTopic(session: Session, topic: String) {
        session.run(
            queryOf(
                """
                INSERT INTO topics (topic, active, current_head, updated_at)
                VALUES (?, ?, ?, NOW())
                """.trimIndent(),
                topic,
                true,
                0,
            ).asUpdate
        )
    }

    override fun updateActive(topic: String, active: Boolean) {
        sessionOf(dataSource).use { session ->
            updateActive(session, topic, active)
        }
    }

    override fun updateActive(session: Session, topic: String, active: Boolean) {
        session.run(
            queryOf(
                """
                    UPDATE topics
                    SET active = ?, updated_at = NOW()
                    WHERE topic = ?
                    """.trimIndent(),
                active,
                topic
            ).asUpdate
        )
    }
}
