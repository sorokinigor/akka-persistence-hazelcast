package akka.persistence.hazelcast.util

import java.util.Map.Entry

import com.hazelcast.map.{EntryBackupProcessor, EntryProcessor}

/**
  * @author Igor Sorokin
  */
@SerialVersionUID(1L)
private[hazelcast] object DeleteProcessor extends EntryProcessor[AnyRef, AnyRef] {

  override def process(entry: Entry[AnyRef, AnyRef]): AnyRef = {
    entry.setValue(null)
    null
  }

  override def getBackupProcessor: EntryBackupProcessor[AnyRef, AnyRef] = null

}
