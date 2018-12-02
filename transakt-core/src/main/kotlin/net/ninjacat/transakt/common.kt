package net.ninjacat.transakt

/**
 * Outcome of transaction stage.
 *
 * This is implementation of Either monad which holds either success or failure
 */
sealed class Result<out F, out S> {
    internal abstract val isSuccess: Boolean
    internal abstract val isFailure: Boolean
    fun isSuccess() = isSuccess
    fun isFailure() = isFailure

    fun <T> fold(onFailure: (failure: F) -> T, onSuccess: (success: S) -> T): T = when (this) {
        is Success -> onSuccess(value)
        is Failure -> onFailure(failure)
    }

    fun <T> flatMap(map: (success: S) -> T): Result<F, T> = when (this) {
        is Success -> Success(map(value))
        is Failure -> this
    }

    fun <T> onFailure(onFailure: (failure: F) -> T): Result<T, S> = when (this) {
        is Failure -> Failure(onFailure(failure))
        is Success -> this
    }

    data class Success<out S> internal constructor(val value: S) : Result<Nothing, S>() {
        override val isSuccess
            get() = true
        override val isFailure
            get() = false
    }

    data class Failure<F> internal constructor(val failure: F) : Result<F, Nothing>() {
        override val isSuccess
            get() = false
        override val isFailure
            get() = true
    }

    companion object {
        /**
         * Creates [Result] with [Failure]
         */
        fun <F> failure(value: F) = Failure(value)

        /**
         * Creates [Result] with [Success]
         */
        fun <S> success(value: S) = Success(value)
    }
}

/**
 * Base interface for transaction stages.
 *
 * Any operation which changes state must be performed as a transaction stage.
 *
 * Implement [apply] method to execute operation in transaction context. `apply()` returns [Result] which is an
 * `Either` type containing either [Result.Success] or [Result.Failure]. When `apply()` returns `Result.Failure`
 * transaction will terminate and automatically roll back any previously completed stages.
 *
 * Implement [compensate] to perform rollback action.
 *
 */
interface TxnStage<out F, out S> {
    /**
     * Optional name for this transaction. Helps with debugging
     */
    fun getName(): String = javaClass.name

    /**
     * Performs actions associated with transaction stage.
     *
     * Do not throw exceptions from this method, always wrap errors in [Result.Failure]
     */
    fun apply(): Result<F, S>

    /**
     * Performs actions which negate previous call to [apply]
     */
    fun compensate()
}
