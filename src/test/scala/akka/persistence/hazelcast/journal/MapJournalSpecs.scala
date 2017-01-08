package akka.persistence.hazelcast.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

/**
  * @author Igor Sorokin
  */
class MapJournalWithDisabledTransactionsSpec extends JournalSpec(ConfigFactory.load("application-transaction-disabled.conf")) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false
}

class MapJournalWithEnabledTransactionsSpec extends JournalSpec(ConfigFactory.load("application-transaction-enabled.conf")) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
