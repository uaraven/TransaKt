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

    override fun <L, R> append(txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>) {
        val envelope = StageEnvelope(txnId, progress, stage)

        val envelopeStr = mapper.writeValueAsString(envelope).replace("\n", "").replace("\r", "")

        lock.write {
            FileOutputStream(getFile(txnId), true).use {
                it.write(envelopeStr.toByteArray(Charsets.UTF_8))
            }
        }
    }

    override fun <L, R> loadStages(txnId: UUID): List<TxnStage<L, R>> {
        val envelopes = lock.read {
            FileInputStream(getFile(txnId)).bufferedReader().use {
                it.readLines().map { line ->
                    mapper.readValue(line, StageEnvelope::class.java)
                }
            }
        }
        return envelopes.filter { it.progress == TxnStageProgress.PostStage }.map {
            it.stage as TxnStage<L, R>
        }
    }

    private fun getFile(txnId: UUID) = storageDir.resolve(txnId.toString()).toFile()

    override fun clear(txnId: UUID) {
        getFile(txnId).delete()
    }

    data class StageEnvelope<L, R>(val txnId: UUID, val progress: TxnStageProgress, val stage: TxnStage<L, R>)
}