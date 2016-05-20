package com.github.rollulus.myprocessor

import io.confluent.connect.avro.AvroConverter
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.connect.data.{ConnectSchema, SchemaBuilder}
import org.apache.kafka.streams.StreamsConfig

import scala.collection.JavaConverters._
import scala.io.StdIn._

/**
  * Created by krism on 4/23/16.
  */
object PublisherRepl {
  def main(): Unit = {
    println("Type in message to publish, type 'END' to finish")
    var userInput = readLine()
    while(!userInput.equals("END")){
      publishToKafka("userInputKey", userInput)
      userInput = readLine()
    }
  }


  val config : Map[String, Object]= Map(

    StreamsConfig.JOB_ID_CONFIG -> "user-input-publisher",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.99.100:2181",
    StreamsConfig.ZOOKEEPER_CONNECT_CONFIG -> "192.168.99.100:2181",

    StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroSerializer],


    StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer],

    "schema.registry.url" -> "http://192.168.99.100:8081/"
  )
  val keySerializer = new StringSerializer
  val valueSerializer = new AvroConverter()
  valueSerializer.configure(Map("schema.registry.url" -> "http://192.168.99.100:8081").asJava, false)
//  new CachedSchemaRegistryClient("192.168.99.100:8081", 100))
  val producer = new KafkaProducer[Array[Byte],Array[Byte]](config.asJava)

  def publishToKafka(key : String, value:Object): Unit = {
    val publishTopic: String = "test"

    val keyBytes = keySerializer.serialize(publishTopic, key)
    val valueBytes = valueSerializer.fromConnectData(publishTopic, SchemaBuilder.string().build(), value)
    producer.send(new ProducerRecord(publishTopic, null, keyBytes, valueBytes))
  }

  def consumeFromKafka(): String = {
    val publishTopic: String = "test"

    val keyBytes = keySerializer.serialize(publishTopic, key)
    val valueBytes = valueSerializer.fromConnectData(publishTopic, SchemaBuilder.string().build(), value)
    producer.send(new ProducerRecord(publishTopic, null, keyBytes, valueBytes))
  }
}
