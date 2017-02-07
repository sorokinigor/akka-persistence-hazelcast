package akka.persistence.hazelcast.snapshot

import akka.persistence.SnapshotMetadata
import akka.persistence.serialization.{Snapshot => PersistenceSnapshot}
import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}

/**
  * @author Igor Sorokin
  */
private[hazelcast] object Snapshot {

  def apply(metadata: SnapshotMetadata, snapshot: PersistenceSnapshot): Snapshot =
    new Snapshot(metadata.timestamp, snapshot)

}

private[hazelcast] final class Snapshot private() extends DataSerializable {
  private var ts: Long = _
  private var persistentSnapshot: PersistenceSnapshot = _

  def this(timestamp: Long, snapshot: PersistenceSnapshot) = {
    this()
    this.ts = timestamp
    this.persistentSnapshot = snapshot
  }

  def timestamp: Long = ts
  def snapshot: PersistenceSnapshot = persistentSnapshot

  override def writeData(out: ObjectDataOutput): Unit = {
    out.writeLong(ts)
    out.writeObject(persistentSnapshot)
  }

  override def readData(in: ObjectDataInput): Unit = {
    this.ts = in.readLong()
    this.persistentSnapshot = in.readObject()
  }

}
