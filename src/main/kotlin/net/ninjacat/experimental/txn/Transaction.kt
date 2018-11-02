package net.ninjacat.experimental.txn

import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

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

class TxnStorageException(override val message: String) : RuntimeException(message)
class TxnRollbackException(val stage: TxnStage<*, *>, override val cause: Throwable) : RuntimeException(cause)

enum class TxnStageProgress {
    PreStage,
    PostStage
}

data class StoredStage<L, R>(val txnId:UUID, val index: Int, val stageProgress: TxnStageProgress, val stage: TxnStage<L, R>)

interface TxnStorage {
    @Throws(TxnStorageException::class)
    fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>)

    fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>>
    fun <L, R> remove(storedStage: StoredStage<L, R>)
    fun clear(txnId: UUID)
}

class TransactionResult<F, S> internal constructor(private val txn: Transaction<F, S>, private val result: Result<F, S>) {
    fun <T> onFailure(block: Transaction<F, S>.(result: Result<F, S>) -> T): Result<T, S> =
            Result.failure(block(txn, result))

    fun <T> onSuccess(block: Transaction<F, S>.(result: Result<F, S>) -> T): Result<F, T> =
            Result.success(block(txn, result))

    operator fun invoke(): Result<F, S> = result
}

@Suppress("UNCHECKED_CAST")
class Transaction<L, R>(private val storage: TxnStorage) {
    private val txnId = UUID.randomUUID()!!

    private val logger = LoggerFactory.getLogger("[txn-manager]")
    private val stageIndex = AtomicInteger(0)

    fun execute(stage: TxnStage<L, R>): R {
        logger.debug("txn[{}] Pre-stage '{}'", txnId, stage.getName())
        storage.append(stageIndex.get(), txnId, TxnStageProgress.PreStage, stage)
        logger.debug("txn[{}] Applying stage '{}'", txnId, stage.getName())
        val result = stage.apply()
        return result.fold({ failure ->
            logger.debug("txn[{}] Stage '{}' failed, result={}", txnId, stage.getName(), failure)
            throw TxnFailedException(failure as Any)
        }, { success ->
            logger.debug("txn[{}] Stage '{}' successful, result={}", txnId, stage.getName(), success)
            logger.debug("txn[{}] Post-stage '{}'", txnId, stage.getName())
            storage.append(stageIndex.getAndIncrement(), txnId, TxnStageProgress.PostStage, stage)
            success
        })
    }

    private fun rollback() {
        logger.debug("txn[{}] Rolling back", txnId)
        val stages = storage.loadStages<L, R>(txnId).groupBy { stage -> stage.index }

        stages.keys.sorted().forEach {
            val stageStateGroup = stages[it]!!.groupBy { stage -> stage.stageProgress }
            val storedStage = stageStateGroup[TxnStageProgress.PostStage]?.first()!!
            try {
                storedStage.stage.compensate()
                storage.remove(storedStage)
            } catch (ex: Exception) {
                throw TxnRollbackException(storedStage.stage, ex)
            }
        }

        storage.clear(txnId)
    }

    /**
     * Begins transaction
     */
    fun begin(block: Transaction<L, R>.() -> R): TransactionResult<L, R> {
        if (stageIndex.get() != 0) {
            throw java.lang.IllegalStateException("Cannot start transaction: another transaction is in progress")
        }
        stageIndex.incrementAndGet()
        logger.debug("txn[{}] Started transaction", txnId)
        return try {
            val result = block(this)
            storage.clear(txnId)
            logger.debug("txn[{}] Transaction committed", txnId)
            TransactionResult(this, Result.success(result))
        } catch (txnEx: Exception) {
            logger.debug("txn[{}] Transaction failed", txnId)
            rollback()
            logger.debug("txn[{}] Transaction rolled back", txnId)
            when (txnEx) {
                is TxnFailedException -> TransactionResult(this, Result.failure(txnEx.causeLeft as L))
                else -> throw IllegalStateException("Unexpected exception, no exception should be thrown by Transaction Stage", txnEx)
            }
        }
    }

    class TxnFailedException(val causeLeft: Any) : Exception()
}