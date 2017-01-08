package akka.persistence.hazelcast.util

import akka.persistence.PersistentRepr
import com.hazelcast.mapreduce.aggregation.PropertyExtractor

/**
  * @author Igor Sorokin
  */
@SerialVersionUID(1L)
private[hazelcast] object LongExtractor extends PropertyExtractor[PersistentRepr, java.lang.Long] {
  override def extract(value: PersistentRepr): java.lang.Long = value.sequenceNr
}
