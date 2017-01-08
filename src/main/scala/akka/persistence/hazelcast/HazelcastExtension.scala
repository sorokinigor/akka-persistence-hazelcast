package akka.persistence.hazelcast

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.persistence.hazelcast.journal.EventId
import akka.persistence.hazelcast.util.SerializerAdapter
import akka.serialization.SerializationExtension
import com.hazelcast.config.{ClasspathXmlConfig, SerializerConfig}
import com.hazelcast.core.{Hazelcast, HazelcastInstance, IMap}
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

private[hazelcast] final class HazelcastExtension(system: ExtendedActorSystem) extends Extension {

  private val serializationExtension = SerializationExtension(system)

  private[hazelcast] val config: Config = system.settings.config.getConfig("hazelcast")
  val hazelcast: HazelcastInstance = {
    val hazelcastConfig = new ClasspathXmlConfig(config.getString("config-file"))
    val serializationConfig = hazelcastConfig.getSerializationConfig
    serializationConfig.addSerializerConfig(createSerializationConfig(classOf[PersistentRepr]))

    Hazelcast.newHazelcastInstance(hazelcastConfig)
  }

  private[hazelcast] lazy val journalMap: IMap[EventId, PersistentRepr] =
    hazelcast.getMap[EventId, PersistentRepr](config.getString("journal.map-name"))

  private[hazelcast] lazy val highestDeletedSequenceNrMap: IMap[String, Long] =
    hazelcast.getMap[String, Long](config.getString("journal.highest-deleted-sequence-number-map-name"))

  private def createSerializationConfig(clazz: Class[_]): SerializerConfig = {
    val serializer = serializationExtension.serializerFor(clazz)
    val config = new SerializerConfig()
    config.setTypeClass(clazz)
    config.setImplementation(new SerializerAdapter(serializer))
  }

}
