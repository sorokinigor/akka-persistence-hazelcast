package akka.persistence.hazelcast

import akka.persistence.PersistentRepr
import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}

/**
  * @author Igor Sorokin
  */
private[hazelcast] object Id {

  def apply(event: PersistentRepr): Id = new Id(event.persistenceId, event.sequenceNr)

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

  override def toString = s"JournalEventId($persistenceId, $sequenceNr)"
}
