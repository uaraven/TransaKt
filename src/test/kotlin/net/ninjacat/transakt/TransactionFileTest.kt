package net.ninjacat.transakt

import net.ninjacat.transakt.storage.FileTransactionStorage
import net.ninjacat.transakt.storage.TransactionLog
import org.hamcrest.Matchers.*
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class TransactionFileTest {

    private lateinit var txnDir: Path
    private lateinit var storage: FileTransactionStorage

    @Before
    fun setUp() {
        txnDir = Files.createTempDirectory("txn-test")
        storage = FileTransactionStorage.build {
            storageDir = txnDir
        }
    }

    @After
    fun tearDown() {
        Files.walk(txnDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach { file -> file.delete() }
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
        assertThat(result().isSuccess, `is`(true))
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
        assertThat(result().isFailure, `is`(true))
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
        javaClass.getResourceAsStream("/txn.log").use { src ->
            FileOutputStream(txnDir.resolve(txnId.toString()).toFile()).use { dst ->
                src.copyTo(dst)
            }
        }

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