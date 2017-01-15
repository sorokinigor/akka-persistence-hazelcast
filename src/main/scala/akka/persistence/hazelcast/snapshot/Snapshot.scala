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
  var timestamp: Long = _
  var snapshot: PersistenceSnapshot = _

  def this(timestamp: Long, snapshot: PersistenceSnapshot) = {
    this()
    this.timestamp = timestamp
    this.snapshot = snapshot
  }

  override def writeData(out: ObjectDataOutput): Unit = {
    out.writeLong(timestamp)
    out.writeObject(snapshot)
  }

  override def readData(in: ObjectDataInput): Unit = {
    this.timestamp = in.readLong()
    this.snapshot = in.readObject()
  }

  override def toString = s"Snapshot($timestamp)"
}
