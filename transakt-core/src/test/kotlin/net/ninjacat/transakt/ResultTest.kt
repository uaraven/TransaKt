package net.ninjacat.transakt

import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Test

@Suppress("USELESS_IS_CHECK")
class ResultTest {
    @Test
    fun successTest() {
        val result = Result.success("good")

        when (result) {
            is Result.Success -> assertThat(result.value, equalTo("good"))
            else -> fail("Result should be successful")
        }
    }

    @Test
    fun failureTest() {
        val result = Result.failure("bad")

        when (result) {
            is Result.Failure -> assertThat(result.failure, equalTo("bad"))
            else -> fail("Result should be failure")
        }
    }

    @Test
    fun resultFlatMap() {
        val result = Result.success("good")

        val newResult = result.flatMap { _ -> 42 }

        when (newResult) {
            is Result.Success -> assertThat(newResult.value, `is`(42))
            else -> fail("Result should be successful")
        }
    }

    @Test
    fun resultOnFailure() {
        val result = Result.failure("bad")

        val newResult = result.onFailure { _ -> 42 }

        when (newResult) {
            is Result.Failure -> assertThat(newResult.failure, `is`(42))
            else -> fail("Result should be failed")
        }
    }

    @Test
    fun resultFold() {
        val good = Result.success("good")
        val bad = Result.failure("bad")

        good.fold({ fail("Should be good") }, { assertThat(it, equalTo("good")) })
        bad.fold({ assertThat(it, equalTo("bad")) }, { fail("Should be bad") })
    }
}