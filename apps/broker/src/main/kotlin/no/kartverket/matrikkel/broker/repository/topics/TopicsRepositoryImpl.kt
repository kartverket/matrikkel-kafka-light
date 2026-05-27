package no.kartverket.matrikkel.broker.repository.topics

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import javax.sql.DataSource

class TopicsRepositoryImpl(val dataSource: DataSource) : TopicsRepository {

    override fun getTopics(): Set<TopicsReadDTO> {
        return sessionOf(dataSource).use { session ->
                getTopics(session)
        }
    }

    override fun getTopics(session: Session): Set<TopicsReadDTO> {
        return session.run(
            queryOf("SELECT * FROM topics")
                .map { row ->
                    TopicsReadDTO(
                        topic = row.string("topic"),
                        active = row.boolean("active"),
                        current_head = row.long("current_head"),
                        updated_at = row.instant("updated_at"),
                    )
                }.asList
        ).toSet()
    }

    override fun addTopic(topic: String) {
        sessionOf(dataSource).use { session ->
           addTopic(topic, session)
        }
    }

    override fun addTopic(topic: String, session: Session) {
        session.run(
            queryOf("""
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
            updateActive(topic, active, session)
        }
    }

    override fun updateActive(topic: String, active: Boolean, session: Session) {
        session.run(
            queryOf("""
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
