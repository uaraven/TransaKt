package net.ninjacat.experimental.txn


sealed class Result<out F, out S> {
    internal abstract val isSuccess: Boolean
    internal abstract val isFailure: Boolean
    fun isSuccess() = isSuccess
    fun isFailure() = isFailure

    fun <T> fold(onFailure: (failure: F) -> T, onSuccess: (success: S) -> T): T = when (this) {
        is Success -> onSuccess(value)
        is Failure -> onFailure(value)
    }

    fun <T> flatMap(map: (success: S) -> T): Result<F, T> = when (this) {
        is Success -> Success(map(value))
        is Failure -> this
    }

    fun <T> onFailure(onFailure: (failure: F) -> T): Result<T, S> = when (this) {
        is Failure -> Failure(onFailure(value))
        is Success -> this
    }

    data class Success<out S> internal constructor(val value: S) : Result<Nothing, S>() {
        override val isSuccess
            get() = true
        override val isFailure
            get() = false
    }

    data class Failure<F> internal constructor(val value: F) : Result<F, Nothing>() {
        override val isSuccess
            get() = false
        override val isFailure
            get() = true
    }

    companion object {
        fun <F> failure(value: F) = Failure(value)
        fun <S> success(value: S) = Success(value)
    }
}


interface TxnStage<L, R> {
    fun getName(): String = javaClass.name
    fun apply(): Result<L, R>
    fun compensate()
}
