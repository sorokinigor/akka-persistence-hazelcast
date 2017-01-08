package akka.persistence.hazelcast.journal

import akka.event.Logging
import akka.persistence.hazelcast.HazelcastExtension
import akka.persistence.hazelcast.util.{DeleteProcessor, LongExtractor}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.mapreduce.aggregation.{Aggregations, Supplier}
import com.hazelcast.nio.serialization.HazelcastSerializationException
import com.hazelcast.query.{Predicate, Predicates}

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Try}

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class MapJournal extends AsyncWriteJournal {
  import scala.collection.JavaConverters._
  import context.dispatcher

  private val logger = Logging.getLogger(context.system, this)
  private val journalMap = HazelcastExtension(context.system).journalMap

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] =
    Future.traverse(messages) { write => doAtomicWrite(write.persistenceId, write.payload) }

  private def doAtomicWrite(persistenceId: String, events: Seq[PersistentRepr]): Future[Try[Unit]] = {
    if (events.size == 1) {
      val promise = Promise.apply[Try[Unit]]()
      val event = events.last
      try {
        journalMap.putAsync(new EventId(persistenceId, event.sequenceNr), event)
          .andThen(new ExecutionCallback[PersistentRepr] {
            override def onResponse(response: PersistentRepr): Unit = promise.success(Try())

            override def onFailure(exception: Throwable): Unit = promise.failure(exception)
          })
      } catch {
        case e: HazelcastSerializationException =>
          promise.success(Failure(e))
      }
      promise.future
    } else {
      //TODO
      Future.failed(new UnsupportedOperationException())
    }
  }

  //TODO keep the highest sequence number
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = Future({
    val idPredicate = persistenceIdPredicate(persistenceId, Predicates.lessEqual("sequenceNr", toSequenceNr))
    val deleted = journalMap.executeOnEntries(DeleteProcessor, idPredicate)
    logger.debug(s"'${deleted.size()}' events to '$toSequenceNr' for '$persistenceId' has been deleted.")
  })

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (recoveryCallback: (PersistentRepr) => Unit): Future[Unit] = Future({
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
    logger.debug(s"'$numberOfEvents' events has been replayed for '$persistenceId' from '$fromSequenceNr' " +
        s"to '$toSequenceNr'")
  })

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = Future({
    val idPredicate = persistenceIdPredicate(persistenceId, Predicates.greaterEqual("sequenceNr", fromSequenceNr))
    val supplier = Supplier.fromPredicate(
      idPredicate,
      Supplier.all[EventId, PersistentRepr, java.lang.Long](LongExtractor)
    )
    val sequenceNumber = journalMap.aggregate(supplier, Aggregations.longMax()).toLong match {
      case Long.MinValue if journalMap.keySet(idPredicate).isEmpty => 0L
      case any => any
    }

    logger.debug(s"Highest sequence number for '$persistenceId' from '$fromSequenceNr' is '$sequenceNumber'.")
    sequenceNumber
  })

  private def persistenceIdPredicate(
      persistenceId: String,
      predicate: Predicate[_, _]
  ): Predicate[EventId, PersistentRepr] =
    Predicates.and(Predicates.equal("persistenceId", persistenceId), predicate)
      .asInstanceOf[Predicate[EventId, PersistentRepr]]

}
