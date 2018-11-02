package net.ninjacat.experimental.txn.storage

import com.fasterxml.jackson.annotation.JsonTypeInfo
import net.ninjacat.experimental.txn.Result
import net.ninjacat.experimental.txn.TxnStage
import net.ninjacat.experimental.txn.TxnStageProgress
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.hamcrest.Matchers.equalTo
import org.junit.Before
import org.junit.Test
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class FileTxnStorageTest {

    private lateinit var txnDir: Path
    private lateinit var txnStorage: FileTxnStorage

    @Before
    fun setUp() {
        txnDir = Files.createTempDirectory("txn-test")
        txnStorage = FileTxnStorage(txnDir)
    }

    @Test
    fun shouldStoreAllStages() {
        val txnId = UUID.randomUUID()
        txnStorage.append(1, txnId, TxnStageProgress.PreStage, IncStage(0))
        txnStorage.append(1, txnId, TxnStageProgress.PostStage, IncStage(1))
        txnStorage.append(2, txnId, TxnStageProgress.PreStage, IncStage(2))
        txnStorage.append(2, txnId, TxnStageProgress.PostStage, IncStage(3))

        val txnFileContents = readTxnFileContents(txnId)
        assertThat(txnFileContents, Matchers.hasSize(4))
        assertThat(txnFileContents[0], Matchers.containsString("\"index\":1"))
        assertThat(txnFileContents[1], Matchers.containsString("\"index\":1"))
        assertThat(txnFileContents[2], Matchers.containsString("\"index\":2"))
        assertThat(txnFileContents[3], Matchers.containsString("\"index\":2"))
    }

    @Test
    fun shouldLoadAllStages() {
        val txnId = UUID.randomUUID()
        val originalStages = mutableListOf<StoredStage<String, Int>>()

        originalStages.add(txnStorage.append(1, txnId, TxnStageProgress.PreStage, IncStage(0)))
        originalStages.add(txnStorage.append(1, txnId, TxnStageProgress.PostStage, IncStage(1)))
        originalStages.add(txnStorage.append(2, txnId, TxnStageProgress.PreStage, IncStage(2)))
        originalStages.add(txnStorage.append(2, txnId, TxnStageProgress.PostStage, IncStage(3)))

        val stages: List<StoredStage<String, Int>> = txnStorage.loadStages(txnId)
        assertThat(stages.size, equalTo(originalStages.size))
        stages.zip(originalStages).forEach { (stage, originalStage) ->
            assertThat(stage, equalTo(originalStage))
        }
    }

    private fun readTxnFileContents(txnId: UUID): List<String> {
        FileInputStream(this.txnDir.resolve(txnId.toString()).toFile()).bufferedReader().useLines {
            return it.toList()
        }
    }

    private fun prepareFileContents(txnId: UUID): List<String> {
        FileInputStream(this.txnDir.resolve(txnId.toString()).toFile()).bufferedReader().useLines {
            return it.toList()
        }
    }

    private data class IncStage(val counter: Int) : TxnStage<String, Int> {
        override fun apply(): Result<String, Int> {
            return Result.success(counter + 1)
        }

        override fun compensate() {
            counter - 1
        }
    }
}