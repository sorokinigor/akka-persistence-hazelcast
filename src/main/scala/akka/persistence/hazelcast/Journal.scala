package akka.persistence.hazelcast

import java.lang
import java.util.Map.Entry

import akka.actor.ActorLogging
import akka.persistence.hazelcast.util.{DeleteProcessor, LongExtractor}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.map.{EntryBackupProcessor, EntryProcessor}
import com.hazelcast.mapreduce.aggregation.{Aggregations, Supplier}
import com.hazelcast.nio.serialization.HazelcastSerializationException
import com.hazelcast.query.{Predicate, Predicates}
import com.hazelcast.transaction.TransactionContext

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class Journal extends AsyncWriteJournal with ActorLogging {
  import context.dispatcher

  import scala.collection.JavaConverters._
  import scala.collection.breakOut

  private val extension = HazelcastExtension(context.system)
  private val journalMap = extension.journalMap
  private val highestDeletedSequenceNrMap = extension.highestDeletedSequenceNrMap

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    Future.traverse(messages) { write => doAtomicWrite(write.persistenceId, write.payload) }

  private def doAtomicWrite(persistenceId: String, events: Seq[PersistentRepr]): Future[Try[Unit]] =
    Future({
      events.size match {
        case 1 =>
          writeSingleEvent(events.last)
        case size if size > 1 && extension.isTransactionEnabled =>
          writeBatchInTransaction(persistenceId, events)
        case size if size > 1 && extension.shouldFailOnNonAtomicPersistAll =>
          throw new UnsupportedOperationException(
            "Transaction are not enabled. Enable 'hazelcast.journal.transaction.enabled' (recommended) or" +
            " disable 'hazelcast.journal.fail-on-non-atomic-persist-all'."
          )
        case _ =>
          writeBatchNonAtomically(events)
      }
    })

  private def writeSingleEvent(event: PersistentRepr): Try[Unit] = {
    try {
      journalMap.set(Id(event), event)
      Journal.emptySuccess
    } catch {
      case e: HazelcastSerializationException =>
        Failure(e)
    }
  }

  private def writeBatchInTransaction(persistenceId: String, events: Seq[PersistentRepr]): Try[Unit] = {
    val context = extension.hazelcast.newTransactionContext(extension.transactionOptions)
    context.beginTransaction()
    try {
      val journalTransactionMap = context.getMap[Id, PersistentRepr](extension.journalMapName)
      events.foreach(event => journalTransactionMap.set(Id(event), event))
      context.commitTransaction()
      Journal.emptySuccess
    } catch {
      case serializationException: HazelcastSerializationException =>
        rollbackTransaction(context, persistenceId, serializationException)
        Failure(serializationException)
      case NonFatal(writeException) =>
        rollbackTransaction(context, persistenceId, writeException)
        throw writeException
    }
  }

  private def rollbackTransaction(context: TransactionContext, persistenceId: String, cause: Throwable) : Unit = {
    log.error(s"Rolling back transaction '${context.getTxnId}' for '$persistenceId'.")
    try {
      context.rollbackTransaction()
    } catch {
      case NonFatal(rollbackException) =>
        log.error(s"Unable to rollback transaction '${context.getTxnId}' for '$persistenceId'.")
        cause.addSuppressed(rollbackException)
    }
  }

  private def writeBatchNonAtomically(events: Seq[PersistentRepr]): Try[Unit] = {
    try {
      val toPut: Map[Id, PersistentRepr] = events
        .map(event => Id(event) -> event)(breakOut)
      journalMap.putAll(toPut.asJava)
      Journal.emptySuccess
    } catch {
      case e: HazelcastSerializationException =>
        Failure(e)
    }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    def createDeleteCallbackOn(promise: Promise[Unit], keys: java.util.Set[Id]) = new ExecutionCallback[Long] {

      override def onResponse(response: Long): Unit = {
        Future({
          promise.tryComplete(Try({
            journalMap.executeOnKeys(keys, DeleteProcessor)
            log.debug(s"'${keys.size()}' events to '$toSequenceNr' for '$persistenceId' have been deleted.")
          }))
        })
      }

      override def onFailure(exception: Throwable): Unit = {
        promise.failure(exception)
      }
    }

    Future({
      val idPredicate = persistenceIdPredicate(persistenceId, Predicates.lessEqual("sequenceNr", toSequenceNr))
      val keys = journalMap.keySet(idPredicate)
      if (!keys.isEmpty) {
        val highestDeletedSequenceNr = keys.asScala
          .maxBy(eventId => eventId.sequenceNr)
          .sequenceNr
        val processor = new HighestSequenceNrProcessor(highestDeletedSequenceNr)
        val promise = Promise[Unit]()
        highestDeletedSequenceNrMap.submitToKey(persistenceId, processor, createDeleteCallbackOn(promise, keys))
        promise
      } else {
        Journal.successfulPromise
      }
    })
    .flatMap(promise => promise.future)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (recoveryCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future({
      val predicate = persistenceIdPredicate(
        persistenceId,
        Predicates.between("sequenceNr", fromSequenceNr, toSequenceNr)
      )
      val numberOfEvents = journalMap.values(predicate)
        .asScala
        .toStream
        .sortBy(event => event.sequenceNr)
        .take(if (max > Int.MaxValue) Int.MaxValue else max.toInt)
        .count(event => { recoveryCallback(event); true })
      log.debug(s"'$numberOfEvents' events has been replayed for '$persistenceId' from '$fromSequenceNr' " +
        s"to '$toSequenceNr'. Max number of events was '$max'.")
    })

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future({
      val idPredicate = persistenceIdPredicate(persistenceId, Predicates.greaterEqual("sequenceNr", fromSequenceNr))
      val supplier: Supplier[Id, PersistentRepr, java.lang.Long] = Supplier.fromPredicate(
        idPredicate,
        Supplier.all[Id, PersistentRepr, java.lang.Long](LongExtractor)
      )
      val aggregator = Aggregations.longMax[Id, lang.Long]()
      val sequenceNumber = journalMap.aggregate[java.lang.Long, java.lang.Long](supplier, aggregator).toLong match {
        case Long.MinValue if journalMap.keySet(idPredicate).isEmpty =>
          highestDeletedSequenceNrMap.getOrDefault(persistenceId, 0L)
        case any => any
      }
      log.debug(s"Highest sequence number for '$persistenceId' from '$fromSequenceNr' is '$sequenceNumber'.")
      sequenceNumber
    })

  private def persistenceIdPredicate(
      persistenceId: String,
      predicate: Predicate[_, _]
  ): Predicate[Id, PersistentRepr] =
    Predicates.and(Predicates.equal("persistenceId", persistenceId), predicate)
      .asInstanceOf[Predicate[Id, PersistentRepr]]

}

private[hazelcast] object Journal {
  private val emptySuccess = Success[Unit]({})
  private val successfulPromise = Promise.successful[Unit]({})
}

@SerialVersionUID(1L)
private final class HighestSequenceNrProcessor(private val newSequenceNr: Long) extends EntryProcessor[String, Long] {

  override def process(entry: Entry[String, Long]): AnyRef = {
    if (newSequenceNr > entry.getValue) {
      entry.setValue(newSequenceNr)
    }
    null
  }

  override def getBackupProcessor: EntryBackupProcessor[String, Long] = null

}
