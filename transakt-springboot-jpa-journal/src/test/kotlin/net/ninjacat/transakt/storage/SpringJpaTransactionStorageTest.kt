package net.ninjacat.transakt.storage

import net.ninjacat.transakt.Result
import net.ninjacat.transakt.Transaction
import net.ninjacat.transakt.TxnStage
import org.hamcrest.Matchers.*
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import javax.transaction.Transactional

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TestSpringConfig::class])
@Transactional
class SpringJpaTransactionStorageTest {

    @Autowired
    private lateinit var repo: TransactionJournalStorage
    @Autowired
    private lateinit var storage: SpringJpaRepositoryStorage

    @Before
    fun setUp() {
    }

    @After
    fun tearDown() {
    }

    @Test
    fun whenAllStagesPass_thenCommit() {
        val manager = Transaction<Throwable, Int>(storage)
        val result = manager.begin {
            testValue = 0
            testValue = execute(Add1(testValue))
            testValue = execute(Add2(testValue))
            testValue = execute(Add3(testValue))
            testValue
        }

        assertThat(testValue, `is`(6))
        assertThat(result().isSuccess(), `is`(true))
        result().fold({
            fail("Expected to succeed")
        }) { outcome ->
            assertThat(outcome, equalTo(testValue))
        }
    }

    @Test
    fun whenStageFails_thenRollback() {
        val manager = Transaction<Throwable, Int>(storage)
        val result = manager.begin {
            testValue = 0
            testValue = execute(Add1(testValue))
            testValue = execute(Add2(testValue))
            execute(Fail(testValue))
            testValue = execute(Add3(testValue))
            testValue
        }

        assertThat(testValue, `is`(0))
        assertThat(result().isFailure(), `is`(true))
    }

    @Test
    fun whenStageFailsAndRollBackFails_thenTransactionlogShouldPersist() {
        val manager = Transaction<Throwable, Int>(storage)
        try {
            val result = manager.begin {
                testValue = 0
                testValue = execute(Add1(testValue))
                testValue = execute(Add2FailRb(testValue))
                execute(Fail(testValue))
                testValue = execute(Add3(testValue))
                testValue
            }

            assertThat(result.hasRollbackFailed(), `is`(true))
        } catch (ignored: Transaction.TxnRollbackException) {
            fail("Rollback should not throw exceptions")
        }
        assertThat(testValue, `is`(not(0)))

        val storedTransactions: List<TransactionLog<Throwable, Int>> = storage.listAllStoredTransactions()
        assertThat(storedTransactions, hasSize(1))
    }

    @Test
    fun whenThereArePendingTransactions_thenRollThemBack() {
        val txnId = UUID.randomUUID()
        repo.storeStage(StoredStage(txnId, 1, TxnStageProgress.PostStage, Add1(0)))
        repo.storeStage(StoredStage(txnId, 2, TxnStageProgress.PostStage, Add2(1)))

        val manager = Transaction<Throwable, Int>(storage)
        testValue = 3

        manager.rollbackAllPendingTransactions { thr, _ ->
            fail("Should not fail")
            throw thr
        }

        assertThat(testValue, `is`(0))
    }


    companion object {
        var testValue = 0
    }

    data class Add1(val value: Int) : TxnStage<Throwable, Int> {
        override fun getName() = "add1"

        override fun compensate() {
            testValue -= 1
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value + 1)
        }
    }

    data class Add2(val value: Int) : TxnStage<Throwable, Int> {
        override fun getName() = "add2"

        override fun compensate() {
            testValue -= 2
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value + 2)
        }
    }


    data class Add2FailRb(val value: Int) : TxnStage<Throwable, Int> {
        override fun getName() = "add2"

        override fun compensate() {
            throw java.lang.IllegalStateException("Failing to execute rollback!")
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value + 2)
        }
    }

    data class Add3(val value: Int) : TxnStage<Throwable, Int> {
        override fun getName() = "add3"

        override fun compensate() {
            testValue -= 3
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value + 3)
        }
    }

    data class Fail(val value: Int) : TxnStage<Throwable, Int> {
        override fun getName() = "failure"

        override fun compensate() {
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.failure(IllegalStateException())
        }
    }
}