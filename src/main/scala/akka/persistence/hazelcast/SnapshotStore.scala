package akka.persistence.hazelcast

import akka.actor.ActorLogging
import akka.persistence.hazelcast.util.DeleteProcessor
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}
import com.hazelcast.query.{Predicate, PredicateBuilder}

import scala.concurrent.Future

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class SnapshotStore extends akka.persistence.snapshot.SnapshotStore with ActorLogging {
  import context.dispatcher

  import scala.collection.JavaConverters._

  private val snapshotMap = HazelcastExtension(context.system).snapshotMap

  override def loadAsync(persistenceId: String,
                         criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    Future({
      snapshotMap.entrySet(createPredicate(persistenceId, criteria)) match {
        case snapshots if snapshots.isEmpty =>
          log.debug(s"No snapshot is available for '$persistenceId'.")
          Option.empty
        case snapshots =>
          val entry = snapshots.asScala
            .maxBy(entry => entry.getKey.sequenceNr)
          val id = entry.getKey
          val snapshot = entry.getValue
          val metadata = SnapshotMetadata(id.persistenceId, id.sequenceNr, snapshot.timestamp)
          log.debug(s"Got '${snapshots.size()}' snapshots for '$persistenceId'. " +
            s"The one with sequenceNr '${id.sequenceNr}' was chosen.")
          Option(SelectedSnapshot(metadata, snapshot.snapshot.data))
      }
    })

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    Future(snapshotMap.set(
      Id(metadata),
      new SnapshotStore.Snapshot(metadata.timestamp, akka.persistence.serialization.Snapshot(snapshot))
    ))

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future(snapshotMap.delete(Id(metadata)))

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future({
      val entries = snapshotMap.executeOnEntries(DeleteProcessor, createPredicate(persistenceId, criteria))
      log.debug(s"'${entries.size()}' snapshots for '$persistenceId' has been deleted.")
    })

  private def createPredicate(persistenceId: String, criteria: SnapshotSelectionCriteria): Predicate[_, _] = {
    val predicateBuilder = new PredicateBuilder()
    val entryObject = predicateBuilder.getEntryObject
    entryObject.key().get("persistenceId").equal(persistenceId)
      .and(entryObject.key().get("sequenceNr").between(criteria.minSequenceNr, criteria.maxSequenceNr))
      .and(entryObject.get("timestamp").between(criteria.minTimestamp, criteria.maxTimestamp))
    predicateBuilder
  }

}

private[hazelcast] object SnapshotStore {

  final class Snapshot private() extends DataSerializable {
    private var ts: Long = _
    private var persistentSnapshot: akka.persistence.serialization.Snapshot = _

    def this(timestamp: Long, snapshot: akka.persistence.serialization.Snapshot) = {
      this()
      this.ts = timestamp
      this.persistentSnapshot = snapshot
    }

    def timestamp: Long = ts
    def snapshot: akka.persistence.serialization.Snapshot = persistentSnapshot

    override def writeData(out: ObjectDataOutput): Unit = {
      out.writeLong(ts)
      out.writeObject(persistentSnapshot)
    }

    override def readData(in: ObjectDataInput): Unit = {
      this.ts = in.readLong()
      this.persistentSnapshot = in.readObject()
    }

  }
}

