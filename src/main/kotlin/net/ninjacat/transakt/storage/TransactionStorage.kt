package net.ninjacat.transakt.storage

import net.ninjacat.transakt.TxnStage
import java.util.*

class TxnStorageException(override val message: String) : RuntimeException(message)

/**
 * Wrapper around stage, containing additional information for transaction log
 */
data class StoredStage<F, S>(val txnId: UUID, val index: Int, val stageProgress: TxnStageProgress, val stage: TxnStage<F, S>)

/**
 * Stored stages for one transaction, identified by UUID
 */
data class TransactionLog<F, S>(val txnId: UUID, val stages: List<StoredStage<F, S>>)

/**
 * Interface for a StageSerializer that might be used by transaction storage.
 *
 * Implement this interface to control how Stages are saved and restored to/from transaction log. This allows to
 * inject all required dependencies into restored stage
 *
 * Exact how stages should be serialized depends on concrete implementation of TransactionStorage used
 */
interface StageSerializer<T> {
    fun <F, S> serialize(stage: TxnStage<F, S>): T
    fun <F, S> deserialize(data: T): TxnStage<F, S>
}

enum class TxnStageProgress {
    PreStage,
    PostStage
}

/**
 * Interface for transaction log storage
 */
interface TransactionStorage {
    /**
     * Appends transaction stage progress to transaction log
     */
    @Throws(TxnStorageException::class)
    fun <F, S> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<F, S>): StoredStage<F, S>

    /**
     * Loads all the stages for transaction with given identifier from transaction log
     */
    fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>>

    /**
     * Removes stage from transaction log. Called by transaction manager when stage was successfully rolled back
     */
    fun <L, R> remove(storedStage: StoredStage<L, R>)

    /**
     * Clears transaction log for given transaction identifier. Called by transaction manager when transaction
     * was completed either successfully or by rolling back all stages
     */
    fun clear(txnId: UUID)

    /**
     * Retrieves all uncompleted transactions in the form of mapping between transaction id and transaction stages.
     *
     * Not all implementation may implement this
     */
    fun <L, R> listAllStoredTransactions(): List<TransactionLog<L, R>>
}