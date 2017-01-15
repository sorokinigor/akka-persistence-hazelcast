package akka.persistence.hazelcast.snapshot

import akka.persistence.hazelcast.HazelcastExtension
import akka.persistence.hazelcast.util.DeleteProcessor
import akka.persistence.serialization.{Snapshot => PersistentSnapshot}
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.hazelcast.query.{Predicate, PredicateBuilder}

import scala.concurrent.Future

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class MapSnapshotStore extends SnapshotStore {
  import scala.collection.JavaConverters._
  import akka.persistence.hazelcast.Id.RichSnapshotMetadata
  import context.dispatcher

  private val snapshotMap = HazelcastExtension(context.system).snapshotMap

  override def loadAsync(persistenceId: String,
                         criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    Future({
      snapshotMap.entrySet(createPredicate(persistenceId, criteria)) match {
        case snapshots if snapshots.isEmpty =>
          Option.empty
        case snapshots =>
          val entry = snapshots.asScala
            .maxBy(entry => entry.getKey.sequenceNr)
          val id = entry.getKey
          val snapshot = entry.getValue
          val metadata = SnapshotMetadata(id.persistenceId, id.sequenceNr, snapshot.timestamp)
          Option(SelectedSnapshot(metadata, snapshot.snapshot.data))
      }
    })
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    Future(snapshotMap.set(metadata.toId, Snapshot(metadata, PersistentSnapshot(snapshot))))

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future(snapshotMap.delete(metadata.toId))

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future(snapshotMap.executeOnEntries(DeleteProcessor, createPredicate(persistenceId, criteria)))

  private def createPredicate(persistenceId: String, criteria: SnapshotSelectionCriteria): Predicate[_, _] = {
    val predicateBuilder = new PredicateBuilder()
    val entryObject = predicateBuilder.getEntryObject
    entryObject.key().get("persistenceId").equal(persistenceId)
      .and(entryObject.key().get("sequenceNr").between(criteria.minSequenceNr, criteria.maxSequenceNr))
      .and(entryObject.get("timestamp").between(criteria.minTimestamp, criteria.maxTimestamp))
    predicateBuilder
  }

}
