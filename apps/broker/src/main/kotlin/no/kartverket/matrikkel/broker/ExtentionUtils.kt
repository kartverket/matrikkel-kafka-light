package no.kartverket.matrikkel.broker

import kotlin.time.Instant


fun Instant?.isAfter(other: Instant): Boolean = if (this == null) false else this > other
fun Instant?.isBefore(other: Instant): Boolean = if (this == null) false else this <= other