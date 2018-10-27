package net.ninjacat.experimental.txn

import arrow.core.Either
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class TransactionTest {

    private lateinit var storage: MemTxnStorage

    @Before
    fun setUp() {
        storage = MemTxnStorage()
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
        assertThat(result.isLeft(), `is`(true))
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

        assertThat(value.get(), `is`(6))
        assertThat(result.isRight(), `is`(true))
        result.fold({
            fail("Expected to succeed")
        }) { outcome ->
            assertThat(outcome, equalTo(value.get()))
        }
    }

    data class Add1(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add1"

        override fun compensate() {
            value.decrementAndGet()
        }

        override fun apply(): Either<Throwable, Int> {
            return Either.right(value.incrementAndGet())
        }
    }

    data class Add2(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add2"

        override fun compensate() {
            value.addAndGet(-2)
        }

        override fun apply(): Either<Throwable, Int> {
            return Either.right(value.addAndGet(2))
        }
    }

    data class Add3(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "add3"

        override fun compensate() {
            value.addAndGet(-3)
        }

        override fun apply(): Either<Throwable, Int> {
            return Either.right(value.addAndGet(3))
        }
    }

    data class Fail(val value: AtomicInteger) : TxnStage<Throwable, Int> {
        override fun getName() = "failure"

        override fun compensate() {
        }

        override fun apply(): Either<Throwable, Int> {
            return Either.left(IllegalStateException())
        }
    }
}