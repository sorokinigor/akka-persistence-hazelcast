package akka.persistence.hazelcast

import akka.persistence.{PersistentRepr, SnapshotMetadata}
import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}

/**
  * @author Igor Sorokin
  */
private[hazelcast] object Id {

  implicit class RichPersistentRepr(val persistentReprId: PersistentRepr) extends AnyVal {

    def toId: Id = new Id(persistentReprId.persistenceId, persistentReprId.sequenceNr)

  }

  implicit class RichSnapshotMetadata(val snapshotMetadata: SnapshotMetadata) extends AnyVal {

    def toId: Id = new Id(snapshotMetadata.persistenceId, snapshotMetadata.sequenceNr)

  }

}

private[hazelcast] final class Id private() extends DataSerializable {
  var persistenceId: String = _
  var sequenceNr: Long = _

  def this(persistenceId: String, sequenceNr: Long) = {
    this()
    this.persistenceId = persistenceId
    this.sequenceNr = sequenceNr
  }

  override def writeData(out: ObjectDataOutput): Unit = {
    out.writeUTF(persistenceId)
    out.writeLong(sequenceNr)
  }

  override def readData(in: ObjectDataInput): Unit = {
    this.persistenceId = in.readUTF()
    this.sequenceNr = in.readLong()
  }

}
