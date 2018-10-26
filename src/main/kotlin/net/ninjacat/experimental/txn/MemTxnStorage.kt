package net.ninjacat.experimental.txn

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class MemTxnStorage() : TxnStorage {
    private val store: ConcurrentMap<UUID, List<StageEnvelope<*, *>>> = ConcurrentHashMap()

    override fun <L, R> append(txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(txnId, progress, stage)
        store.merge(txnId, listOf(envelope)) {e1, e2 ->
            e1 + e2
        }
    }

    override fun <L, R> loadStages(txnId: UUID): List<TxnStage<L, R>> {
        val envelopes = store.getOrDefault(txnId, emptyList())
        return envelopes.asSequence().filter { it.progress == TxnStageProgress.PostStage }.map {
            it.stage as TxnStage<L, R>
        }.toList()
    }

    override fun clear(txnId: UUID) {
        store.remove(txnId)
    }


    data class StageEnvelope<L, R>(val txnId: UUID, val progress: TxnStageProgress, val stage: TxnStage<L, R>)
}