package com.lifion.consumer

import com.lifion.sordb.TransactionChanges
import io.confluent.kafka.serializers._
import org.apache.avro.generic._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.ValueMapper
import java.util.Arrays
import java.util.Properties

import org.apache.kafka.streams


object AuditConsumer {
//  def saveToCassandra(events:KStream[(Array[Byte], GenericRecord)]) = {
//    TransactionChanges.saveToCassandra(events)
////    RawEvents.saveToCassandra(events)
////    EventsByEntity.saveToCassandra(events)
////    EventsByTopicAndOrUser.saveToCassandra(events)
////    EventsByMiniapp.saveToCassandra(events)
//  }

  def transform(messages:KStream[String, Object]):KStream[Array[Byte], GenericRecord] = {
    messages.map((k, v)=> {
      val message = v.asInstanceOf[(Array[Byte], Object)]
      new streams.KeyValue(message._1, message._2.asInstanceOf[GenericRecord])
    })
  }

  def getKafkaStream(builder: KStreamBuilder, schemaRegistryUrl:String, topics:String) = {
//     val kafkaParams = Map[String, String](
    //     "metadata.broker.list" -> brokerList,
    //     "schema.registry.url" -> schemaRegistryUrl
    //   )
    // KafkaUtils.createDirectStream[String, Object, StringDecoder, CustomKafkaAvroDecoder](
    //   ssc, kafkaParams, topicsSet)
    builder.stream(topics.split(","):_*)
  }
}
