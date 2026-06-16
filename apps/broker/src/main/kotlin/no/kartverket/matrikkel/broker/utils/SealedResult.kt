package no.kartverket.matrikkel.broker.utils

sealed interface SealedResult<out T : Any> {
    data class Success<T : Any>(val value: T) : SealedResult<T>
    data class Failure(val error: Exception) : SealedResult<Nothing>

    companion object {
        fun <T : Any> success(value: T) = Success(value)
        fun failure(value: Exception) = Failure(value)
        fun failure(value: String) = Failure(Exception(value))
    }
}

fun <T : Any> runCatching(block: () -> T): SealedResult<T> {
    return try {
        SealedResult.success(block())
    } catch (e: Exception) {
        SealedResult.failure(e)
    }
}