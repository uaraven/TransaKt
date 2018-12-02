package net.ninjacat.transakt.storage

import net.ninjacat.transakt.Result
import net.ninjacat.transakt.TxnStage
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test
import java.util.*

class JsonSerializerTest {

    private val serializer = JsonSerializer()

    @Test
    fun shouldSerializeToSingleLine() {
        val stage = Stage(1, "All world is a stage", UUID.randomUUID())

        val serialized = serializer.serialize(stage)

        assertThat(serialized.contains("\\s"), `is`(false))
        assertThat(serialized.contains("\n"), `is`(false))
        assertThat(serialized.contains("\r"), `is`(false))
    }

    @Test
    fun shouldSerializeAndDeserialize() {
        val stage = Stage(1, "All world is a stage", UUID.randomUUID())

        val serialized = serializer.serialize(stage)

        val deserialized = serializer.deserialize<String, Int>(serialized) as Stage

        assertThat(deserialized, equalTo(stage))
    }

    data class Stage(val id: Long, val value: String, val id2: UUID) : TxnStage<String, Int> {
        override fun apply(): Result<String, Int> {
            return Result.success(10)
        }

        override fun compensate() {
        }
    }
}