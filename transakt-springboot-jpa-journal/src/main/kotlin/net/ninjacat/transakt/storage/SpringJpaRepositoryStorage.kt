package net.ninjacat.transakt.storage

import net.ninjacat.transakt.TxnStage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*
import kotlin.streams.asSequence

@Component
class SpringJpaRepositoryStorage @Autowired constructor(private val storage: TransactionJournalStorage) : TransactionStorage {

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>): StoredStage<L, R> {
        val storedStage = StoredStage(txnId, index, progress, stage)
        storage.storeStage(storedStage)
        return storedStage
    }

    override fun <F, S> loadStages(txnId: UUID): List<StoredStage<F, S>> {
        val stages: List<StoredStage<F, S>> = storage.getStages<F, S>(txnId).asSequence().toList()

        return stages.asSequence()
                .sortedWith(kotlin.Comparator { e1, e2 -> e1.index.compareTo(e2.index) })
                .toList()
    }

    override fun <L, R> remove(storedStage: StoredStage<L, R>) {
        storage.remove(storedStage)
    }

    override fun clear(txnId: UUID) {
        storage.deleteAllForTransaction(txnId)
    }

    override fun <L, R> listAllStoredTransactions(): List<TransactionLog<L, R>> {
        return storage.listTransactionIds()
                .asSequence()
                .map { txnId -> TransactionLog(txnId, loadStages<L, R>(txnId)) }
                .toList()
    }
}
