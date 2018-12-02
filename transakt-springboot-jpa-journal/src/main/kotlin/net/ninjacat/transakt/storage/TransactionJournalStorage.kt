package net.ninjacat.transakt.storage

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import net.ninjacat.transakt.TxnStage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import java.io.Serializable
import java.util.*
import java.util.stream.Stream
import javax.persistence.Embeddable
import javax.persistence.EmbeddedId
import javax.persistence.Entity


/**
 * Provides serialization/deserialization for [TxnStage] classes that is compatible with [SpringJpaRepositoryStorage]
 */
class JsonSerializer : StageSerializer<String> {
    private data class StageWrapper<F, S>(val stage: TxnStage<F, S>)

    private val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .enableDefaultTyping()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    override fun <F, S> serialize(stage: TxnStage<F, S>): String {
        return mapper.writeValueAsString(StageWrapper(stage))
    }

    override fun <F, S> deserialize(data: String): TxnStage<F, S> {
        @Suppress("UNCHECKED_CAST")
        val wrapper = mapper.readValue(data, StageWrapper::class.java) as StageWrapper<F, S>
        return wrapper.stage
    }

}


@Embeddable
data class StageIdentity(val index: Int = 0, val txnId: UUID = UUID.randomUUID(), val progress: TxnStageProgress = TxnStageProgress.PreStage) : Serializable

@Entity
data class StageEntity(@EmbeddedId val identity: StageIdentity = StageIdentity(), val stage: String = "") {

    fun <F, S> toStoredStage(): StoredStage<F, S> =
            StoredStage(identity.txnId, identity.index, identity.progress, serializer.deserialize(stage))

    companion object {
        private val serializer = JsonSerializer()

        fun <F, S> createFrom(storedStage: StoredStage<F, S>): StageEntity = StageEntity(
                StageIdentity(storedStage.index, storedStage.txnId, storedStage.stageProgress),
                serializer.serialize(storedStage.stage))

    }
}

@Repository
interface StageRepository : CrudRepository<StageEntity, StageIdentity> {
    fun findByIdentityTxnId(txnId: UUID): List<StageEntity>
    fun deleteByIdentityTxnId(txnId: UUID)
}

@Component
open class TransactionJournalStorage @Autowired constructor(private val stageRepository: StageRepository) {
    fun listTransactionIds(): Stream<UUID> = stageRepository
            .findAll()
            .map { it -> it.identity.txnId }
            .toSet()
            .stream()

    fun <F, S> storeStage(stage: StoredStage<F, S>) {
        stageRepository.save(StageEntity.createFrom(stage))
    }

    fun <F, S> getStages(txnId: UUID): Stream<StoredStage<F, S>> =
            stageRepository.findByIdentityTxnId(txnId).map { it -> it.toStoredStage<F, S>() }.stream()

    fun <F, S> remove(storedStage: StoredStage<F, S>) = stageRepository.delete(StageEntity.createFrom(storedStage))

    fun deleteAllForTransaction(txnId: UUID) {
        stageRepository.deleteByIdentityTxnId(txnId)
    }
}