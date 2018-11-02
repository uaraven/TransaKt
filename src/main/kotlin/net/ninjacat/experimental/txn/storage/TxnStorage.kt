package net.ninjacat.experimental.txn.storage

import net.ninjacat.experimental.txn.TxnStage
import net.ninjacat.experimental.txn.TxnStageProgress
import java.util.*

class TxnStorageException(override val message: String) : RuntimeException(message)

data class StoredStage<L, R>(val txnId:UUID, val index: Int, val stageProgress: TxnStageProgress, val stage: TxnStage<L, R>)

interface TxnStorage {
    @Throws(TxnStorageException::class)
    fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>): StoredStage<L, R>

    fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>>
    fun <L, R> remove(storedStage: StoredStage<L, R>)
    fun clear(txnId: UUID)
}