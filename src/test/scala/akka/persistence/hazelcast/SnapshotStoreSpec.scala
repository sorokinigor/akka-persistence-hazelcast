package akka.persistence.hazelcast

import com.typesafe.config.ConfigFactory

/**
  * @author Igor Sorokin
  */
class SnapshotStoreSpec extends akka.persistence.snapshot.SnapshotStoreSpec(ConfigFactory.load())
