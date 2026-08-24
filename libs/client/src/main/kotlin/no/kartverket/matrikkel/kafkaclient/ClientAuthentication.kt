package no.kartverket.matrikkel.kafkaclient

interface ClientAuthentication {
    fun getAuthenticationHeaderValue(): String
}