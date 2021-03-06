package net.ninjacat.transakt.storage

import net.ninjacat.transakt.Result
import net.ninjacat.transakt.TxnStage
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.hamcrest.Matchers.equalTo
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class FileTransactionStorageTest {

    private lateinit var txnDir: Path
    private lateinit var txnStorage: FileTransactionStorage

    @Before
    fun setUp() {
        txnDir = Files.createTempDirectory("txn-test")
        txnStorage = FileTransactionStorage(txnDir, JsonSerializer())
    }

    @After
    fun tearDown() {
        Files.walk(txnDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach { file -> file.delete() }
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

    @Test
    fun shouldSkipLoadingOfRolledBackStages() {
        val txnId = UUID.randomUUID()
        val originalStages = mutableListOf<StoredStage<String, Int>>()

        originalStages.add(txnStorage.append(1, txnId, TxnStageProgress.PreStage, IncStage(0)))
        originalStages.add(txnStorage.append(1, txnId, TxnStageProgress.PostStage, IncStage(1)))
        originalStages.add(txnStorage.append(2, txnId, TxnStageProgress.PreStage, IncStage(2)))
        originalStages.add(txnStorage.append(2, txnId, TxnStageProgress.PostStage, IncStage(3)))
        txnStorage.appendTxnState(2, txnId, StoredStageProgress.Removed, JsonSerializer().serialize(IncStage(3)))

        val nonRolledBackOriginalStages = originalStages.filter { stage -> stage.index == 1 }
        val stages: List<StoredStage<String, Int>> = txnStorage.loadStages(txnId)

        assertThat(stages.size, equalTo(nonRolledBackOriginalStages.size))
        stages.zip(nonRolledBackOriginalStages).forEach { (stage, originalStage) ->
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