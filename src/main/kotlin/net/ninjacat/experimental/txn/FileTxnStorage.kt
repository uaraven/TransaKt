package net.ninjacat.experimental.txn

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class FileTxnStorage(val storageDir: Path) : TxnStorage {
    private val mapper = ObjectMapper().registerModule(KotlinModule())

    private val lock = ReentrantReadWriteLock()

    fun <L,R> StageEnvelope<L, R>.toJsonString() = mapper.writeValueAsString(this)
            .replace("\n", "")
            .replace("\r", "")

    override fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(index, txnId, progress, stage)

        lock.write {
            FileOutputStream(getFile(txnId), true).use {
                it.write(envelope.toJsonString().toByteArray(Charsets.UTF_8))
            }
        }
    }

    override fun <L, R> remove(storedStage: StoredStage<L, R>) {
        lock.write {
            val inFile = getFile(storedStage.txnId)
            val envelopes = FileInputStream(inFile).bufferedReader().use {
                it.readLines().map { line ->
                    mapper.readValue(line, StageEnvelope::class.java)
                }
            }
            val outFile = getFile(storedStage.txnId, "new")
            FileOutputStream(outFile, true).use {outStream ->
                envelopes.filter {
                    it.index != storedStage.index
                }.forEach {
                    outStream.write(it.toJsonString().toByteArray(Charsets.UTF_8))
                }

            }
            inFile.delete()
            outFile.renameTo(inFile)
        }

    }

    @Suppress("UNCHECKED_CAST")
    override fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>> {
        val envelopes = lock.read {
            FileInputStream(getFile(txnId)).bufferedReader().use {
                it.readLines().map { line ->
                    mapper.readValue(line, StageEnvelope::class.java)
                }
            }
        }
        return envelopes
                .asSequence()
                .sortedWith(kotlin.Comparator { e1, e2 -> e1.index.compareTo(e2.index) })
                .filter { it.progress == TxnStageProgress.PostStage }.map {
                    StoredStage(txnId, it.index, it.progress, it.stage as TxnStage<L, R>)
                }.toList()
    }

    private fun getFile(txnId: UUID, suffix: String = "") = storageDir.resolve(txnId.toString() + suffix).toFile()

    override fun clear(txnId: UUID) {
        getFile(txnId).delete()
    }

    data class StageEnvelope<L, R>(val index: Int, val txnId: UUID, val progress: TxnStageProgress, val stage: TxnStage<L, R>)
}