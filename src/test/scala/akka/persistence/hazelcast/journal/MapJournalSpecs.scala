package akka.persistence.hazelcast.journal


import scala.concurrent.duration._
import akka.actor.Actor
import akka.persistence.JournalProtocol._
import akka.persistence.journal.JournalSpec
import akka.persistence.{AtomicWrite, CapabilityFlag, DeleteMessagesSuccess, PersistentRepr}
import akka.testkit.{EventFilter, TestProbe}
import com.typesafe.config.ConfigFactory

/**
  * @author Igor Sorokin
  */
class MapJournalWithDisabledTransactionsSpec extends JournalSpec(ConfigFactory.load()) {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false

  "journal" must {
    "fail on 'persistAll'" in {
      val probe = TestProbe()
      val events = 1L to 5L map(sequenceNr => PersistentRepr(
        payload = s"a-$sequenceNr", sequenceNr = sequenceNr, persistenceId = pid, sender = probe.ref,
        writerUuid = writerUuid))

      journal ! WriteMessages(List(AtomicWrite(events)), probe.ref, actorInstanceId)
      probe.expectMsgPF() {
        case WriteMessagesFailed(exception) =>
          exception shouldBe an [UnsupportedOperationException]
      }
    }
  }

}

class MapJournalWithEnabledTransactionsSpec
  extends JournalSpec(ConfigFactory.load("application-transaction-enabled.conf"))
  with RejectingNonSerializableForPersistAllSpec {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

}

class MapJournalWithNonAtomicPersistAllSpec
  extends JournalSpec(ConfigFactory.load("application-non-atomic-persist-all.conf"))
  with RejectingNonSerializableForPersistAllSpec {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

}

trait RejectingNonSerializableForPersistAllSpec {
  this: JournalSpec =>

  "journal" must {
    "reject non-serializable events" in EventFilter[java.io.NotSerializableException]().intercept {
      val payload = new Object()
      val sequenceNumberRange = 1L to 5L
      val notSerializableEvents = sequenceNumberRange
        .map(sequenceNr => PersistentRepr(payload = payload, sequenceNr = sequenceNr, persistenceId = pid,
          sender = Actor.noSender, writerUuid = writerUuid))

      val probe = TestProbe()
      journal ! WriteMessages(List(AtomicWrite(notSerializableEvents)), probe.ref, actorInstanceId)

      probe.expectMsg(WriteMessagesSuccessful)
      sequenceNumberRange.foreach(sequenceNumber => {
        probe.expectMsgPF() {
          case rejectedMessage: WriteMessageRejected =>
            val rejected = rejectedMessage.message
            rejected.payload.asInstanceOf[AnyRef] should be theSameInstanceAs payload
            rejected.sequenceNr shouldBe sequenceNumber
            rejected.persistenceId shouldBe pid
        }
      })
    }
    "delete keys only once" in {
      val deleteStatusReceiver = TestProbe()
      val deleteMessage = DeleteMessagesTo(pid, 3, deleteStatusReceiver.ref)
      val deleteSubscriber = TestProbe()
      subscribe[DeleteMessagesTo](deleteSubscriber.ref)
      for (i <- 0 to 1) {
        journal ! deleteMessage
        deleteSubscriber.expectMsg(deleteMessage)
        deleteStatusReceiver.expectMsg(DeleteMessagesSuccess(deleteMessage.toSequenceNr))
      }

      val replayMessageReceiver = TestProbe()
      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, pid, replayMessageReceiver.ref)
      for (i <- 4 to 5) {
        replayMessageReceiver.expectMsg(replayedMessage(i))
      }
      deleteStatusReceiver.expectNoMsg(200.millis)
    }
  }

}
