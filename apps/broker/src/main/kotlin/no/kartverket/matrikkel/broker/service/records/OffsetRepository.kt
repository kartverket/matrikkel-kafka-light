package no.kartverket.matrikkel.broker.service.records

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.kartverket.matrikkel.broker.domain.Topic
import no.kartverket.matrikkel.broker.service.records.LeaseRepository.LeaseStatus
import no.kartverket.matrikkel.kafkaclient.InitialOffsetPolicy
import org.intellij.lang.annotations.Language

object OffsetRepository {
    context(tx: TransactionalSession, _: LeaseStatus.Acquired)
    fun getOffset(
        topic: Topic,
        consumerGroup: String,
        initialOffsetPolicy: InitialOffsetPolicy,
    ): Long {
        val existingOffset = getOffsetOrNull(topic, consumerGroup)
        if (existingOffset != null) return existingOffset

        createOffset(topic, consumerGroup, initialOffsetPolicy)
        return getOffset(topic, consumerGroup, initialOffsetPolicy)
    }

    context(tx: TransactionalSession)
    private fun getOffsetOrNull(
        topic: Topic,
        consumerGroup: String,
    ): Long? {
        val paramMapTopicConsumer = mapOf(
            "topic" to topic.name,
            "consumer_group" to consumerGroup,
        )

        @Language("SQL")
        val query = queryOf(
            """
            SELECT committed_sequence
            FROM consumer_offsets
            WHERE topic = :topic AND consumer_group = :consumer_group
        """.trimIndent(), paramMapTopicConsumer
        )
            .map { it.long("committed_sequence") }
            .asSingle

        return tx.run(query)
    }

    context(tx: TransactionalSession)
    private fun createOffset(
        topic: Topic,
        consumerGroup: String,
        initialOffsetPolicy: InitialOffsetPolicy
    ) {
        val paramMapInitialOffset = mapOf(
            "topic" to topic.name,
            "consumer_group" to consumerGroup,
            "initial_offset" to when (initialOffsetPolicy) {
                InitialOffsetPolicy.EARLIEST -> 0L
                InitialOffsetPolicy.LATEST -> RecordsRepository.currentHeadForTopic(topic)
            },
        )

        @Language("SQL")
        val query = queryOf(
            """
           INSERT INTO consumer_offsets (topic, consumer_group, committed_sequence)
           VALUES(:topic, :consumer_group, :initial_offset)
       """.trimIndent(), paramMapInitialOffset
        ).asExecute

        tx.run(query)
    }
}
