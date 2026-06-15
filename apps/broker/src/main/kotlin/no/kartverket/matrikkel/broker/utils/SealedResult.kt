package no.kartverket.matrikkel.broker.utils

sealed interface SealedResult<out T : Any> {
    data class Success<T : Any>(val value: T) : SealedResult<T>
    data class Failure<T : Any>(val error: Exception) : SealedResult<T>

    companion object {
        fun <T : Any> success(value: T) = Success(value)
        fun <T : Any> failure(value: Exception) = Failure<T>(value)
        fun <T : Any> failure(value: String) = Failure<T>(Exception(value))
    }
}

fun <T : Any> runCatching(block: () -> T): SealedResult<T> {
    return try {
        SealedResult.success(block())
    } catch (e: Exception) {
        SealedResult.failure(e)
    }
}