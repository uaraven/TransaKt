package net.ninjacat.transakt.storage

import net.ninjacat.transakt.Result
import net.ninjacat.transakt.TxnStage
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.instanceOf
import org.junit.Assert.assertThat
import org.junit.Test
import java.util.*

class StageEntityTest {
    private val stage = Stage(1, "test", UUID.randomUUID())
    private val serializer = JsonSerializer()
    private val stageSerialized = serializer.serialize(stage)

    @Test
    fun shouldConvertStoredStageToStageEntity() {
        val storedStage = StoredStage(UUID.randomUUID(), 1, TxnStageProgress.PreStage, stage)

        val entity = StageEntity.createFrom(storedStage)

        assertThat(entity.identity.txnId, equalTo(storedStage.txnId))
        assertThat(entity.identity.index, equalTo(storedStage.index))
        assertThat(entity.identity.progress, equalTo(storedStage.stageProgress))
        assertThat(entity.stage, equalTo(stageSerialized))
    }

    @Test
    fun shouldConvertStageEntityToStoredStage() {
        val entity = StageEntity(
                StageIdentity(1, UUID.randomUUID(), TxnStageProgress.PostStage),
                stageSerialized
        )

        val storedStage = entity.toStoredStage<String, Int>()

        assertThat(storedStage.txnId, equalTo(entity.identity.txnId))
        assertThat(storedStage.index, equalTo(entity.identity.index))
        assertThat(storedStage.stageProgress, equalTo(entity.identity.progress))
        assertThat(storedStage.stage, instanceOf(Stage::class.java))
        assertThat(storedStage.stage as Stage, equalTo(stage))
    }

    data class Stage(val id: Long, val value: String, val id2: UUID) : TxnStage<String, Int> {
        override fun apply(): Result<String, Int> {
            return Result.success(10)
        }

        override fun compensate() {
        }
    }
}