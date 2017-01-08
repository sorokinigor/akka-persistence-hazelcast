package akka.persistence.hazelcast.journal

import com.hazelcast.nio.serialization.DataSerializable
import com.hazelcast.nio.{ObjectDataInput, ObjectDataOutput}

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class EventId private() extends DataSerializable {
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
