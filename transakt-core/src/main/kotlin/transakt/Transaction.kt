package net.ninjacat.transakt

import net.ninjacat.transakt.storage.StoredStage
import net.ninjacat.transakt.storage.TransactionLog
import net.ninjacat.transakt.storage.TransactionStorage
import net.ninjacat.transakt.storage.TxnStageProgress
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Represents outcome of whole transaction.
 * Transaction may complete with following possible outcomes:
 *
 *   - Success
 *   - Failed and rolled back
 *   - Failed and failed to roll back
 *
 * Additionally there is one outcome caused by breach of contract by stage: Exception is thrown from the state,
 * in this case there is no Failure to provide, that's why Failure part of the result is nullable.
 * Rollback attempt will be made in this case as well, and it may succeed or fail.
 *
 * TransactionOutcome is also a function that returns result of the last transaction stage. (Or just call [result] method.)
 *
 * ```
 *    val outcome = transaction.begin {
 *      ...
 *      execute(lastStage)
 *    }
 *    val lastStageResult = outcome()
 *    // or
 *    val lastStageResultAgain = outcome.get()
 * ```
 *
 *
 */
class TransactionOutcome<F, S> internal constructor(
        private val txn: Transaction<F, S>,
        private val result: Result<F?, S>,
        private val rollbackFailure: Throwable? = null) {

    /**
     * Executes code if rollback has failed
     */
    fun <T> onRollbackFailure(block: Transaction<F, S>.(rollbackFailure: Throwable) -> Unit) {
        if (hasRollbackFailed()) block(txn, rollbackFailure!!)
    }

    /**
     * Executes code if transaction completed unsuccessfully
     */
    fun <T> onFailure(block: Transaction<F, S>.(failure: F?) -> T): Result<T, S> =
            result().onFailure { block(txn, it) }

    /**
     * Executes code if transaction completed successfully
     */
    fun <T> onSuccess(block: Transaction<F, S>.(result: S) -> T): Result<F?, T> =
            result().flatMap { block(txn, it) }

    /**
     * Retrieves result of the last transaction stage
     */
    fun result(): Result<F?, S> = result

    operator fun invoke(): Result<F?, S> = result()

    fun hasRollbackFailed() = rollbackFailure != null
}

/**
 * Transaction manager. Allows to execute state-changing code as transaction stages and
 * rollback completed stages in the even of failure.
 */
class Transaction<F, S>(private val storage: TransactionStorage) {
    private val txnId = UUID.randomUUID()!!

    private val logger = LoggerFactory.getLogger("[txn-manager]")
    private val stageIndex = AtomicInteger(0)

    /**
     * Executes transaction stage
     */
    fun execute(stage: TxnStage<F, S>): S {
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

    private fun <L, R> rollbackStages(txnId: UUID, stageList: List<StoredStage<L, R>>) {
        val stages = stageList.groupBy { stage -> stage.index }
        stages.keys.sorted().reversed().forEach {
            val stageStateGroup = stages[it]!!.groupBy { stage -> stage.stageProgress }
            val storedStage = Optional.ofNullable(stageStateGroup[TxnStageProgress.PostStage]?.first())
            try {
                storedStage.ifPresent { st ->
                    st.stage.compensate()
                    storage.remove(st)
                }
            } catch (ex: Exception) {
                throw TxnRollbackException(storedStage.map { it.stage }.get(), ex)
            }
        }
        storage.clear(txnId)
    }

    private fun rollback(): Result<TxnRollbackException, UUID> =
            try {
                rollbackStages(txnId, storage.loadStages<F, S>(txnId))
                Result.success(txnId)
            } catch (ex: TxnRollbackException) {
                logger.error("txn[$txnId] Stage ${ex.stage} rollback failed. Transaction log for this transaction is kept, " +
                        "so that another attempt can be made. " +
                        "No further automatic rollbacks will be done for this transaction", ex)
                Result.failure(ex)
            }

    /**
     * Begins transaction. Executes provided `block` inside transaction context. Any operations that
     * can fail and must be compensated during rollback must be executed with [execute] method.
     *
     * Once transaction is started with `begin` method it cannot be started again. [IllegalStateException] will be
     * thrown if `begin` is called more than once.
     *
     * Transaction is not thread-safe and relies that all stages are executed one-by-one
     */
    @Suppress("UNCHECKED_CAST")
    fun begin(block: Transaction<F, S>.() -> S): TransactionOutcome<F, S> {
        if (stageIndex.get() != 0) {
            throw java.lang.IllegalStateException("Cannot start transaction: another transaction is in progress")
        }
        stageIndex.incrementAndGet()
        logger.debug("txn[{}] Started transaction", txnId)
        return try {

            val result = block(this)
            storage.clear(txnId)
            logger.debug("txn[{}] Transaction committed", txnId)
            TransactionOutcome(this, Result.success(result))

        } catch (txnEx: Exception) {
            logger.debug("txn[{}] Transaction failed, rolling back", txnId)

            val rollbackResult = rollback()

            val originalCause = when (txnEx) {
                is TxnFailedException -> Result.failure(txnEx.causeLeft as F)
                else -> {
                    logger.error("Unexpected exception, no exception should be thrown by Transaction Stage", txnEx)
                    Result.failure(null)
                }
            }

            when (rollbackResult) {
                is Result.Failure -> TransactionOutcome(this, originalCause, rollbackResult.failure)
                else -> TransactionOutcome(this, originalCause)
            }
        }
    }

    /**
     * Helper method which will go through all non-committed transactions and attempt to roll them back.
     *
     * If any of the roll backs fails again you can decide what to do in `onFailure` handler
     */
    fun rollbackAllPendingTransactions(onFailure: Transaction<F, S>.(failure: Throwable, stage: TxnStage<F, S>) -> Unit) {
        val log: List<TransactionLog<F, S>> = storage.listAllStoredTransactions()
        log.forEach { logItem ->
            try {
                rollbackStages(logItem.txnId, logItem.stages)
            } catch (ex: TxnRollbackException) {
                @Suppress("UNCHECKED_CAST")
                onFailure(this, ex.cause, ex.stage as TxnStage<F, S>)
            }
        }
    }

    class TxnRollbackException(val stage: TxnStage<*, *>, override val cause: Throwable) : RuntimeException(cause)
    private class TxnFailedException(val causeLeft: Any) : Exception()
}