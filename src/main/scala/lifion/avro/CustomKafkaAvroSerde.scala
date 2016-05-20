//package com.lifion.avro
//
//import io.confluent.kafka.schemaregistry.storage.serialization.Serializer
//import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
//import org.apache.kafka.common.serialization.Deserializer
//
//class CustomKafkaAvroSerde() extends Serde[Array[Byte]] {
//  override def serializer() : Serializer[Array[Byte]] = new KafkaAvroDeserializer
//
//  override def deserializer() : Deserializer[(Array[Byte], Object)] = new KafkaAvroSerializer
//
//  // override def fromBytes(bytes:Array[Byte]):(Array[Byte], Object) = {
//  //   (bytes, super.fromBytes(bytes));
//  // }
//}
