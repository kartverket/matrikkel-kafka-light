package no.kartverket.matrikkel.broker.repository.topics

import no.kartverket.matrikkel.broker.repository.DTO
import java.time.Instant

data class TopicsReadDTO (val topic: String,
                          val active: Boolean,
                          val current_head: Long,
                          val updated_at : Instant) : DTO {
}