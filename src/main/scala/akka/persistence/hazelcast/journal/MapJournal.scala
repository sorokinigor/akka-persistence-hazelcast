package akka.persistence.hazelcast.journal

import akka.actor.ActorLogging
import akka.persistence.hazelcast.{HazelcastExtension, Id}
import akka.persistence.hazelcast.util.{DeleteProcessor, LongExtractor}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.hazelcast.mapreduce.aggregation.{Aggregations, Supplier}
import com.hazelcast.nio.serialization.HazelcastSerializationException
import com.hazelcast.query.{Predicate, Predicates}
import com.hazelcast.transaction.TransactionContext

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * @author Igor Sorokin
  */
private[hazelcast] object MapJournal {
  private val emptySuccess = Success({})
}

private[hazelcast] final class MapJournal extends AsyncWriteJournal with ActorLogging {
  import scala.collection.JavaConverters._
  import scala.collection.breakOut
  import akka.persistence.hazelcast.Id.RichPersistentRepr
  import context.dispatcher

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
      journalMap.set(event.toId, event)
      MapJournal.emptySuccess
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
      events.foreach(event => journalTransactionMap.set(event.toId, event))
      context.commitTransaction()
      MapJournal.emptySuccess
    } catch {
      case serializationException: HazelcastSerializationException =>
        rollbackTransaction(context, persistenceId, serializationException)
        Failure(serializationException)
      case writeException: Exception =>
        rollbackTransaction(context, persistenceId, writeException)
        throw writeException
    }
  }

  private def rollbackTransaction(context: TransactionContext, persistenceId: String, cause: Exception) : Unit = {
    log.error(s"Rolling back transaction '${context.getTxnId}' for '$persistenceId'.")
    try {
      context.rollbackTransaction()
    } catch {
      case rollbackException: Exception =>
        log.error(s"Unable to rollback transaction '${context.getTxnId}' for '$persistenceId'.")
        cause.addSuppressed(rollbackException)
    }
  }

  private def writeBatchNonAtomically(events: Seq[PersistentRepr]): Try[Unit] = {
    try {
      val toPut: Map[Id, PersistentRepr] = events.map(event => event.toId -> event)(breakOut)
      journalMap.putAll(toPut.asJava)
      MapJournal.emptySuccess
    } catch {
      case e: HazelcastSerializationException =>
        Failure(e)
    }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future({
      val idPredicate = persistenceIdPredicate(persistenceId, Predicates.lessEqual("sequenceNr", toSequenceNr))
      val keys = journalMap.keySet(idPredicate)
      if (!keys.isEmpty) {
        val highestDeletedSequenceNr = keys.asScala
          .maxBy(eventId => eventId.sequenceNr)
          .sequenceNr
        highestDeletedSequenceNrMap.set(persistenceId, highestDeletedSequenceNr)
        journalMap.executeOnKeys(keys, DeleteProcessor)
      }
      log.debug(s"'${keys.size()}' events to '$toSequenceNr' for '$persistenceId' has been deleted.")
    })

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (recoveryCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future({
      val predicate = persistenceIdPredicate(
        persistenceId,
        Predicates.between("sequenceNr", fromSequenceNr, toSequenceNr)
      )
      val eventKeys = journalMap.keySet(predicate)
      val numberOfEvents = journalMap.getAll(eventKeys)
        .values()
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
      val supplier = Supplier.fromPredicate(
        idPredicate,
        Supplier.all[Id, PersistentRepr, java.lang.Long](LongExtractor)
      )
      val sequenceNumber = journalMap.aggregate(supplier, Aggregations.longMax()).toLong match {
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
