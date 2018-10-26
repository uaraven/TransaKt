package net.ninjacat.experimental.txn

import arrow.core.Either
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

interface TxnStage<L, R> {
    fun getName(): String = javaClass.name
    fun apply(): Either<L, R>
    fun compensate()
}

class TxnStorageException(override val message: String) : RuntimeException(message)
enum class TxnStageProgress {
    PreStage,
    PostStage
}

data class StoredStage<L,R>(val index: Int, val stageProgress: TxnStageProgress, val stage: TxnStage<L, R>)

interface TxnStorage {
    @Throws(TxnStorageException::class)
    fun <L, R> append(index: Int, txnId: UUID, progress: TxnStageProgress, stage: TxnStage<L, R>)
    fun <L, R> loadStages(txnId: UUID): List<StoredStage<L, R>>
    // TODO: remove stages when rollback of one is completed
    fun clear(txnId: UUID)
}

@Suppress("UNCHECKED_CAST")
class TxnManager<L, R>(private val storage: TxnStorage) {
    private val txnId = UUID.randomUUID()!!

    private val logger = LoggerFactory.getLogger("[txn-manager]")
    private val stages = mutableListOf<TxnStage<L, R>>()
    private val stageIndex = AtomicInteger(0)

    fun execute(stage: TxnStage<L, R>): R {
        logger.debug("txn[{}] Pre-stage '{}'", txnId, stage.getName())
        storage.append(stageIndex.get(), txnId, TxnStageProgress.PreStage, stage)
        logger.debug("txn[{}] Applying stage '{}'", txnId, stage.getName())
        val result = stage.apply()
        return result.fold({ failure ->
            logger.debug("txn[{}] Stage '{}' failed, result={}", txnId, stage.getName(), failure)
            throw TxnFailedException(failure as Any)
        }, { success ->
            logger.debug("txn[{}] Stage '{}' successful, result={}", txnId, stage.getName(), success)
            stages.add(stage)
            logger.debug("txn[{}] Post-stage '{}'", txnId, stage.getName())
            storage.append(stageIndex.getAndIncrement(), txnId, TxnStageProgress.PostStage, stage)
            success
        })
    }

    private fun rollback() {
        logger.debug("txn[{}] Rolling back", txnId)
        stages.reverse()
        stages.forEach {stage ->
            logger.debug("txn[{}] Compensating for stage '{}'", txnId, stage.getName())
            stage.compensate()
        }
    }

    fun begin(block: TxnManager<L, R>.() -> R): Either<L, R> {
        if (stageIndex.get() != 0) {
            throw java.lang.IllegalStateException("Cannot start another transaction")
        }
        stageIndex.incrementAndGet()
        logger.debug("txn[{}] Started transaction", txnId)
        return try {
            val result = block(this)
            storage.clear(txnId)
            logger.debug("txn[{}] Transaction committed", txnId)
            Either.right(result)
        } catch (txnEx: Exception) {
            logger.debug("txn[{}] Transaction failed", txnId)
            rollback()
            logger.debug("txn[{}] Transaction rolled back", txnId)
            when (txnEx) {
                is TxnFailedException -> Either.left(txnEx.causeLeft as L)
                else -> throw IllegalStateException("Unexpected exception, all exception should be handled as Either", txnEx)
            }
        }
    }

    class TxnFailedException(val causeLeft: Any) : Exception()
}