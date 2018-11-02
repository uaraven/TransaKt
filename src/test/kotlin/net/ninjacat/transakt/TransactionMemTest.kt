package net.ninjacat.transakt

import net.ninjacat.transakt.storage.MemTransactionStorage
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.Matchers.nullValue
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class TransactionMemTest {

    private lateinit var storage: MemTransactionStorage

    @Before
    fun setUp() {
        storage = MemTransactionStorage()
    }

    @Test
    fun whenStageFails_thenRollback() {
        val manager = Transaction<Throwable, Int>(storage)
        val value = AtomicInteger(0)
        val result = manager.begin {
            execute(Add1(value))
            execute(Add2(value))
            execute(Fail(value))
            execute(Add3(value))
        }

        assertThat(value.get(), `is`(0))
        assertThat(result().isFailure, `is`(true))
    }

    @Test
    fun whenAllStagesPass_thenCommit() {
        val manager = Transaction<Throwable, Int>(storage)
        val value = AtomicInteger(0)
        val result = manager.begin {
            execute(Add1(value))
            execute(Add2(value))
            execute(Add3(value))
        }

        val res = result.result()
        when (res) {
            is Result.Success -> println(res.value + 1)
            is Result.Failure -> throw res.failure!!
        }

        assertThat(value.get(), `is`(6))
        assertThat(result().isSuccess, `is`(true))
        result().fold({
            fail("Expected to succeed")
        }) { outcome ->
            assertThat(outcome, equalTo(value.get()))
        }
    }

    @Test
    fun whenStageThrowsException_thenRollbackAndComplain() {
        val manager = Transaction<Throwable, Int>(storage)
        val value = AtomicInteger(0)
        val result = manager.begin {
            execute(Add1(value))
            execute(Add2(value))
            execute(Throw(value))
        }

        assertThat(result.hasRollbackFailed(), `is`(false))

        val res = result.result()
        when (res) {
            is Result.Success -> fail("Should have failed without reason")
            is Result.Failure -> assertThat(res.failure, `is`(nullValue()))
        }
    }

    data class Add1(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add1"

        override fun compensate() {
            value.decrementAndGet()
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value.incrementAndGet())
        }
    }

    data class Add2(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add2"

        override fun compensate() {
            value.addAndGet(-2)
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value.addAndGet(2))
        }
    }

    data class Add3(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add3"

        override fun compensate() {
            value.addAndGet(-3)
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.success(value.addAndGet(3))
        }
    }

    data class Fail(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "failure"

        override fun compensate() {
        }

        override fun apply(): Result<Throwable, Int> {
            return Result.failure(IllegalStateException())
        }
    }

    data class Throw(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "failure"

        override fun compensate() {
        }

        override fun apply(): Result<Throwable, Int> {
            throw IllegalStateException()
        }
    }
}