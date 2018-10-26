package net.ninjacat.experimental.txn

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class MemTxnStorage() : TxnStorage {
    private val store: ConcurrentMap<UUID, List<StageEnvelope<*, *>>> = ConcurrentHashMap()

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(index, txnId, progress, stage)
        store.merge(txnId, listOf(envelope)) { e1, e2 ->
            e1 + e2
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>> {
        val envelopes = store.getOrDefault(txnId, emptyList())
        return envelopes.asSequence()
                .sortedWith(kotlin.Comparator { e1, e2 -> e1.index.compareTo(e2.index) })
                .filter { it.progress == TxnStageProgress.PostStage }.map {
                    StoredStage(it.index, it.progress, it.stage as TxnStage<L, R>)
                }.toList()
    }

    override fun clear(txnId: UUID) {
        store.remove(txnId)
    }


    data class StageEnvelope<L, R>(val index: Int, val txnId: UUID, val progress: TxnStageProgress, val stage: TxnStage<L, R>)
}