package akka.persistence.hazelcast

import akka.persistence.{PersistentRepr, SnapshotMetadata}
import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}

/**
  * @author Igor Sorokin
  */
private[hazelcast] object Id {

  implicit def persistentRepr2Id(persistentRepr: PersistentRepr): Id =
    new Id(persistentRepr.persistenceId, persistentRepr.sequenceNr)

  implicit def snapshotMetadata2Id(snapshotMetadata: SnapshotMetadata): Id =
    new Id(snapshotMetadata.persistenceId, snapshotMetadata.sequenceNr)

}

private[hazelcast] final class Id private() extends DataSerializable {
  private var id: String = _
  private var sequenceNumber: Long = _

  def this(persistenceId: String, sequenceNr: Long) = {
    this()
    this.id = persistenceId
    this.sequenceNumber = sequenceNr
  }

  def persistenceId: String = id
  def sequenceNr: Long = sequenceNumber

  override def writeData(out: ObjectDataOutput): Unit = {
    out.writeUTF(id)
    out.writeLong(sequenceNumber)
  }

  override def readData(in: ObjectDataInput): Unit = {
    this.id = in.readUTF()
    this.sequenceNumber = in.readLong()
  }

}
