package akka.persistence.hazelcast

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.persistence.hazelcast.snapshot.Snapshot
import akka.persistence.hazelcast.util.SerializerAdapter
import akka.persistence.serialization.{Snapshot => PersistenceSnapshot}
import akka.serialization.SerializationExtension
import com.hazelcast.config.{ClasspathXmlConfig, SerializerConfig}
import com.hazelcast.core.{Hazelcast, HazelcastInstance, IMap}
import com.hazelcast.transaction.TransactionOptions
import com.typesafe.config.Config

/**
  * @author Igor Sorokin
  */
object HazelcastExtension extends ExtensionId[HazelcastExtension] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): HazelcastExtension = new HazelcastExtension(system)

  override def lookup(): ExtensionId[_ <: Extension] = HazelcastExtension

  /**
   * Java API: retrieve the Count extension for the given system.
   */
  override def get(system: ActorSystem): HazelcastExtension = super.get(system)
}

final class HazelcastExtension private[hazelcast](system: ExtendedActorSystem) extends Extension {

  private val serializationExtension = SerializationExtension(system)

  private[hazelcast] val config: Config = system.settings.config.getConfig("hazelcast")

  val hazelcast: HazelcastInstance = {
    val hazelcastConfig = new ClasspathXmlConfig(config.getString("config-file"))
    val serializationConfig = hazelcastConfig.getSerializationConfig
    serializationConfig.addSerializerConfig(createSerializationConfig(classOf[PersistentRepr]))
    serializationConfig.addSerializerConfig(createSerializationConfig(classOf[PersistenceSnapshot]))

    Hazelcast.newHazelcastInstance(hazelcastConfig)
  }

  private[hazelcast] val journalMapName = config.getString("journal.map-name")
  private[hazelcast] lazy val journalMap: IMap[Id, PersistentRepr] = hazelcast.getMap(journalMapName)

  private[hazelcast] lazy val highestDeletedSequenceNrMap: IMap[String, Long] =
    hazelcast.getMap(config.getString("journal.highest-deleted-sequence-number-map-name"))

  private[hazelcast] lazy val snapshotMap: IMap[Id, Snapshot] =
    hazelcast.getMap(config.getString("snapshot-store.map-name"))

  private[hazelcast] val shouldFailOnNonAtomicPersistAll: Boolean =
    config.getBoolean("journal.fail-on-non-atomic-persist-all")

  private[hazelcast] val isTransactionEnabled: Boolean = config.getBoolean("journal.transaction.enabled")
  private[hazelcast] val transactionOptions: TransactionOptions = {
    val transactionConfig: Config = config.getConfig("journal.transaction")
    val options = new TransactionOptions()
    val transactionType = TransactionOptions.TransactionType.valueOf(transactionConfig.getString("type"))
    options.setTransactionType(transactionType)
    if (transactionType == TransactionOptions.TransactionType.TWO_PHASE) {
      options.setDurability(transactionConfig.getInt("durability"))
    }
    val timeout = transactionConfig.getDuration("timeout")
    options.setTimeout(timeout.toMillis, TimeUnit.MILLISECONDS)
    options
  }

  private[hazelcast] val writeDispatcher = system.dispatchers.lookup(config.getString("write-dispatcher"))

  private def createSerializationConfig(clazz: Class[_]): SerializerConfig = {
    val serializer = serializationExtension.serializerFor(clazz)
    val config = new SerializerConfig()
    config.setTypeClass(clazz)
    config.setImplementation(new SerializerAdapter(serializer))
  }

}
