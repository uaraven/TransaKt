package net.ninjacat.transakt.storage

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import net.ninjacat.transakt.TxnStage
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.streams.asSequence

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

/**
 * Provides serialization/deserialization for [TxnStage] classes that is compatible with [FileTransactionStorage]
 */
class JsonSerializer : StageSerializer<String> {
    private data class StageWrapper<F, S>(val stage: TxnStage<F, S>)

    private val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .enableDefaultTyping()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    override fun <F, S> serialize(stage: TxnStage<F, S>): String {
        return mapper.writeValueAsString(StageWrapper(stage)).asSingleLine()
    }

    override fun <F, S> deserialize(data: String): TxnStage<F, S> {
        @Suppress("UNCHECKED_CAST")
        val wrapper = mapper.readValue(data, StageWrapper::class.java) as StageWrapper<F, S>
        return wrapper.stage
    }

}

/**
 * Append-only plain-text transaction log with storage on local file system.
 *
 * This Transaction storage saves transaction stages as a single-line JSON objects. This sets some requirements for
 * serializers. Any serializer used with this storage must convert [TxnStage] to ASCII-only single-line (no \n or \r) string.
 *
 * [JsonSerializer] provides option to serialize [TxnStage] as JSON which is safe to use with FileTransactionStorage. In fact
 * it is default serializer when creating storage using [FileTransactionStorage.build]
 *
 * @param storageDir Directory where transaction log will be stored. *Never* use same directory for transactions of different types
 * @param serializer Custom serializer. Implementation of [StageSerializer] can be used to correctly initialize [TxnStage] objects restored from transaction log
 * @param alwaysSync Performs FS sync() operation after every write, ensuring that data is persisted on disk. Will affect performance,
 *                     but its effect might be negligible when transaction includes multiple network calls.
 */
class FileTransactionStorage(private val storageDir: Path, private val serializer: StageSerializer<String>, private val alwaysSync: Boolean = false) : TransactionStorage {
    private val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .enableDefaultTyping()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val lock = ReentrantReadWriteLock()

    private fun StageEnvelope.toJsonString() = mapper.writeValueAsString(this).asSingleLine()

    internal fun appendTxnState(index: Int, txnId: UUID, progress: StoredStageProgress, stage: String) {
        val envelope = StageEnvelope(index, txnId, progress, stage)

        lock.write {
            FileOutputStream(getFile(txnId), true).use {
                it.write(envelope.toJsonString().toByteArray(Charsets.UTF_8))
                it.write('\n'.toInt())
                if (alwaysSync) {
                    it.fd.sync()
                }
            }
        }
    }

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>): StoredStage<L, R> {
        appendTxnState(index, txnId, StoredStageProgress.fromTxnStage(progress), serializer.serialize(stage))
        return StoredStage(txnId, index, progress, stage)
    }

    /**
     * File-based transaction log appends stage flagged as Removed, instead of modifying whole transaction file
     */
    override fun <F, S> remove(storedStage: StoredStage<F, S>) {
        appendTxnState(storedStage.index, storedStage.txnId, StoredStageProgress.Removed, serializer.serialize(storedStage.stage))
    }

    override fun <F, S> loadStages(txnId: UUID): List<StoredStage<F, S>> {
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
                    (StoredStage(it.txnId, it.index, it.progress.toTxnStage(), serializer.deserialize<F, S>(it.stage)))
                }
                .toList()
    }

    override fun <L, R> listAllStoredTransactions(): List<TransactionLog<L, R>> {
        return Files.list(storageDir)
                .asSequence()
                .map { file -> UUID.fromString(file.fileName.toString()) }
                .map { txnId -> TransactionLog(txnId, loadStages<L, R>(txnId)) }
                .toList()
    }

    private fun getFile(txnId: UUID, suffix: String = "") = storageDir.resolve(txnId.toString() + suffix).toFile()

    override fun clear(txnId: UUID) {
        getFile(txnId).delete()
    }

    internal data class StageEnvelope(val index: Int, val txnId: UUID, val progress: StoredStageProgress, val stage: String)


    companion object {
        /**
         * Builder DSL for [FileTransactionStorage].
         *
         * Allows to configure storage directory, serializer and FileSystem sync.
         *
         * By default serializer is set to [JsonSerializer] and Fs sync is disabled
         */
        inline fun build(block: Builder.() -> Unit) = Builder().apply(block).build()
    }

    class Builder {
        var storageDir: Path? = null
        private var serializer: StageSerializer<String>? = JsonSerializer()
        private var alwaysSync: Boolean = false

        fun build() = FileTransactionStorage(storageDir!!, serializer!!, alwaysSync)
    }
}

internal fun String.asSingleLine() = this.replace("\n", "").replace("\r", "")