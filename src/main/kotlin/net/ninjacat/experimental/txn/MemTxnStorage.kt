package net.ninjacat.experimental.txn

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class MemTxnStorage() : TxnStorage {
    data class TxnKey(val txnId: UUID, val index: Int)
    private val store: ConcurrentMap<TxnKey, List<StageEnvelope<*, *>>> = ConcurrentHashMap()

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(index, txnId, progress, stage)
        store.merge(TxnKey(txnId, index) , listOf(envelope)) { e1, e2 ->
            e1 + e2
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>> {
        val envelopes = store.entries.asSequence()
                .filter { entry -> entry.key.txnId == txnId }
                .flatMap { it.value.asSequence() }.toList()
        return envelopes.asSequence()
                .sortedWith(kotlin.Comparator { e1, e2 -> e1.index.compareTo(e2.index) })
                .filter { it.progress == TxnStageProgress.PostStage }.map {
                    StoredStage(txnId, it.index, it.progress, it.stage as TxnStage<L, R>)
                }.toList()
    }

    override fun <L, R> remove(storedStage: StoredStage<L, R>) {
        store.remove(TxnKey(storedStage.txnId, storedStage.index))
    }

    override fun clear(txnId: UUID) {
        val keysToRemove = store.keys.filter { it.txnId == txnId }
        keysToRemove.forEach {
            store.remove(it)
        }
    }


    data class StageEnvelope<L, R>(val index: Int, val txnId: UUID, val progress: TxnStageProgress, val stage: TxnStage<L, R>)
}