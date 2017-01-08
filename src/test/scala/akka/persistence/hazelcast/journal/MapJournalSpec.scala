package akka.persistence.hazelcast.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

/**
  * @author Igor Sorokin
  */
class MapJournalSpec extends JournalSpec(ConfigFactory.load()) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false
}
