package no.kartverket.no.kartverket.matrikkel.broker.testutils

import assertk.Assert
import assertk.assertions.support.expected
import assertk.assertions.support.show
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

fun Assert<Instant>.isApproxNow(leeway: Duration = 5.seconds): Unit = given { actual ->
    val now = Clock.System.now()
    val start = now - leeway
    val end = now + leeway
    if (actual in start..end) return
    expected("to be within ${show(leeway)} of ${show(now)}, but was ${show(actual)}")
}