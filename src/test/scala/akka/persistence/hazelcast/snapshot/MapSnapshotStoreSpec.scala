package akka.persistence.hazelcast.snapshot

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

/**
  * @author Igor Sorokin
  */
class MapSnapshotStoreSpec extends SnapshotStoreSpec(ConfigFactory.load())
