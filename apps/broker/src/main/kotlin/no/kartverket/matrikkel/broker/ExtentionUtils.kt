package no.kartverket.matrikkel.broker

import kotlin.time.Instant


// Lages fordi kotlin.Instant ikke har disse, og håndtering av "før" og "etter" på nullable typer er tricky
fun Instant?.isAfter(other: Instant): Boolean = if (this == null) false else this > other
fun Instant?.isBefore(other: Instant): Boolean = if (this == null) false else this <= other