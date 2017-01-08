package akka.persistence.hazelcast.util

import akka.serialization.Serializer
import com.hazelcast.nio.serialization.ByteArraySerializer

/**
  * @author Igor Sorokin
  */
private[hazelcast] final class SerializerAdapter(private val serializer: Serializer)
  extends ByteArraySerializer[AnyRef] {

  override def write(target: AnyRef): Array[Byte] = serializer.toBinary(target)

  override def read(buffer: Array[Byte]): AnyRef = serializer.fromBinary(buffer)

  override def getTypeId: Int = serializer.identifier

  override def destroy(): Unit = {}
}
