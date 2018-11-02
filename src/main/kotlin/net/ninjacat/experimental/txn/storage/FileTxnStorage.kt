package net.ninjacat.experimental.txn.storage

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import net.ninjacat.experimental.txn.TxnStage
import net.ninjacat.experimental.txn.TxnStageProgress
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

internal enum class StoredStageProgress {
    PreStage,
    PostStage,
    Removed;

    fun toTxnStage() = when (this) {
        PreStage -> TxnStageProgress.PreStage
        PostStage -> TxnStageProgress.PostStage
        else -> throw IllegalStateException("Cannot convert 'Removed' stage to TxnStageProgress")
    }

    companion object {
        fun fromTxnStage(txnStage: TxnStageProgress) = when (txnStage) {
            TxnStageProgress.PreStage -> PreStage
            TxnStageProgress.PostStage -> PostStage
        }
    }
}

class FileTxnStorage(private val storageDir: Path) : TxnStorage {
    private val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .enableDefaultTyping()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val lock = ReentrantReadWriteLock()

    private fun <L, R> StageEnvelope<L, R>.toJsonString() = mapper.writeValueAsString(this)
            .replace("\n", "")
            .replace("\r", "")

    internal fun <L, R> appendTxnState(index: Int, txnId: UUID, progress: StoredStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(index, txnId, progress, stage)

        lock.write {
            FileOutputStream(getFile(txnId), true).use {
                it.write(envelope.toJsonString().toByteArray(Charsets.UTF_8))
                it.write('\n'.toInt())
            }
        }
    }

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>): StoredStage<L, R> {
        appendTxnState(index, txnId, StoredStageProgress.fromTxnStage(progress), stage)
        return StoredStage(txnId, index, progress, stage)
    }

    /**
     * File-based transaction log appends storage flagged as Removed instead of rewriting whole transaction file
     */
    override fun <L, R> remove(storedStage: StoredStage<L, R>) {
        appendTxnState(storedStage.index, storedStage.txnId, StoredStageProgress.Removed, storedStage.stage)
    }

    override fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>> {
        val envelopes = lock.read {
            FileInputStream(getFile(txnId)).bufferedReader().use {
                it.readLines().map { line ->
                    mapper.readValue(line, StageEnvelope::class.java)
                }
            }
        }
        val filteredEntries = envelopes.asSequence()
                .groupBy { it.index } // group all stages of the same transaction
                .filter { entry ->
                    entry.value.none { stage -> stage.progress == StoredStageProgress.Removed }
                }

        return filteredEntries
                .flatMap { entry -> entry.value }
                .sortedWith(kotlin.Comparator { e1, e2 -> e1.index.compareTo(e2.index) })
                .map {
                    @Suppress("UNCHECKED_CAST")
                    StoredStage(it.txnId, it.index, it.progress.toTxnStage(), it.stage as TxnStage<L, R>)
                }
                .toList()
    }

    private fun getFile(txnId: UUID, suffix: String = "") = storageDir.resolve(txnId.toString() + suffix).toFile()

    override fun clear(txnId: UUID) {
        getFile(txnId).delete()
    }

    internal data class StageEnvelope<L, R>(val index: Int, val txnId: UUID, val progress: StoredStageProgress, val stage: TxnStage<L, R>)
}