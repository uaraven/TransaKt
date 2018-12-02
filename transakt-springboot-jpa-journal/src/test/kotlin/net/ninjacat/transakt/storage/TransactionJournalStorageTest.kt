package net.ninjacat.transakt.storage

import net.ninjacat.transakt.Result
import net.ninjacat.transakt.TxnStage
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.hasSize
import org.junit.Assert.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import javax.transaction.Transactional
import kotlin.streams.asSequence

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TestSpringConfig::class])
@Transactional
class TransactionJournalStorageTest {

    @Autowired
    private lateinit var stageRepo: StageRepository
    @Autowired
    private lateinit var repo: TransactionJournalStorage

    private val serializer = JsonSerializer()

    @Test
    fun shouldReadStagesFromDb() {
        val txn1Id = UUID.randomUUID()
        val txn2Id = UUID.randomUUID()

        generateTestStages(txn1Id, txn2Id)

        val stages = this.repo.listTransactions<String, Int>().asSequence().toList()
        assertThat(stages, hasSize(3))
    }

    @Test
    fun shouldReadStagesByTransactionId() {
        val txn1Id = UUID.randomUUID()
        val txn2Id = UUID.randomUUID()

        generateTestStages(txn1Id, txn2Id)

        val stages = this.repo.getStages<String, Int>(txn1Id).asSequence().toList()

        assertThat(stages, hasSize(2))
        assertThat(stages[0].index, `is`(1))
        assertThat(stages[1].index, `is`(2))
    }

    @Test
    fun shouldDeleteStages() {
        val txn1Id = UUID.randomUUID()
        val txn2Id = UUID.randomUUID()
        val stage = Stage(1, "", UUID.randomUUID())

        val identity = StageIdentity(1, txn1Id, TxnStageProgress.PreStage)

        saveTestStages(txn1Id, txn2Id, stage)

        assertThat(stageRepo.existsById(identity), `is`(true))

        repo.remove(StoredStage(txn1Id, 1, TxnStageProgress.PreStage, stage))

        assertThat(stageRepo.existsById(identity), `is`(false))
    }

    @Test
    fun shouldSaveStoredStage() {
        val txn1Id = UUID.randomUUID()
        val stage = Stage(1, "", UUID.randomUUID())
        val identity = StageIdentity(1, txn1Id, TxnStageProgress.PreStage)

        assertThat(stageRepo.existsById(identity), `is`(false))

        repo.storeStage(StoredStage(txn1Id, 1, TxnStageProgress.PreStage, stage))

        assertThat(stageRepo.existsById(identity), `is`(true))
    }

    @Test
    fun shouldDeleteAllStagesOfTransaction() {
        val txn1Id = UUID.randomUUID()
        val txn2Id = UUID.randomUUID()

        generateTestStages(txn1Id, txn2Id)

        repo.deleteAllForTransaction(txn1Id)

        val txnCount = repo.listTransactions<String, Int>().count()

        assertThat(txnCount, `is`(1L))
    }

    private fun generateTestStages(txn1Id: UUID, txn2Id: UUID) {
        val stage = Stage(1, "", UUID.randomUUID())
        saveTestStages(txn1Id, txn2Id, stage)
    }

    private fun saveTestStages(txn1Id: UUID, txn2Id: UUID, stage: Stage) {
        stageRepo.save(StageEntity(StageIdentity(1, txn1Id, TxnStageProgress.PreStage), serializer.serialize(stage)))
        stageRepo.save(StageEntity(StageIdentity(2, txn1Id, TxnStageProgress.PostStage), serializer.serialize(stage)))
        stageRepo.save(StageEntity(StageIdentity(1, txn2Id, TxnStageProgress.PreStage), serializer.serialize(stage)))
    }

    data class Stage(val id: Long, val value: String, val id2: UUID) : TxnStage<String, Int> {
        override fun apply(): Result<String, Int> {
            return Result.success(10)
        }

        override fun compensate() {
        }
    }
}