package com.lifion.consumer

import java.util.Properties

import com.github.rollulus.myprocessor.PropertiesHelper
import com.lifion.avro.{EventAvroConverter, SorPayloadAvroConverter}
import com.lifion.event.Event
import com.lifion.sordb.{TransactionChanges, TransactionChangesTable}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.KStreamBuilder
import io.confluent.kafka.serializers.{AbstractKafkaAvroDeserializer, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.api.ConnectEmbedded
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.storage.{KafkaConfigStorage, KafkaOffsetBackingStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._

class AuditConsumerJob {

}

object AuditConsumerJob {
  private val log: Logger = LoggerFactory.getLogger(classOf[AuditConsumerJob])

  def main(args: Array[String]) {

    var CASSANDRA_HOST = "localhost"
    var KAFKA_BROKERS_LIST = "localhost:2181"
    var SCHEMA_REGISTRY_URL = "localhost:8081"
    var TOPIC_LIST = "test"

    if (args.length != 0) {
      CASSANDRA_HOST = args(0)
      KAFKA_BROKERS_LIST = args(1)
      SCHEMA_REGISTRY_URL = args(2)
      TOPIC_LIST = args(3)
    }

//    val connect: ConnectEmbedded = createCassandraConnector(CASSANDRA_HOST, SCHEMA_REGISTRY_URL)

    val connect: ConnectEmbedded = null

    val props: StreamsConfig = GetKafkaStreamConfiguration(KAFKA_BROKERS_LIST)

    val builder = new KStreamBuilder

    // read from kafka
    builder.stream(new StringDeserializer, new KafkaAvroDeserializer, TOPIC_LIST.split(","): _*)
      .map[String, Object]((k,v)=> {
      println(k)
      KeyValue.pair(k,v)
    })
      // transform to cassandra record
      .map[Array[Byte], GenericRecord]((k: String, v: Object) => {
      val message = v.asInstanceOf[(Array[Byte], Object)]
      new KeyValue(message._1, message._2.asInstanceOf[GenericRecord])
    })
      .filter((k, v) => isSorEvent(v))
      .mapValues[Event]((v) => EventAvroConverter.convert(v))
      .flatMapValues[TransactionChangesTable](
      (event) => {
        var index = -1
        val sorPayload = SorPayloadAvroConverter.convert(event.payload)
        sorPayload.sorDataTransactions.map((change) => {
          index += 1
          TransactionChanges.convertToTable(event, change, index)
        }).asJava
      })
      // save to back to kafka, ready to transform
      .to("audit-save-to-cassandra")

    val streams = new KafkaStreams(builder, props)

    try {
      streams.start()
      //connect.start()
    }
    catch {
      case e: Throwable =>
        log.error("Stopping the application due to streams initialization error ", e)
        connect.stop()
    }
    Runtime.getRuntime.addShutdownHook(new Thread(() => connect.stop()))

    try {
      Thread.sleep(Integer.MAX_VALUE)
      connect.awaitStop()
      log.info("Connect closed cleanly...")
    } finally {
      streams.close()
      log.info("Streams closed cleanly...")
    }
  }

  private def createCassandraConnector(cassandraBootstrapServers: String, schemaRegistryUrl: String): ConnectEmbedded = {
    val workerProps = PropertiesHelper.create(Map(
      DistributedConfig.GROUP_ID_CONFIG -> "audit-connect",
      //      WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> cassandraBootstrapServers,
            WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.99.100:2181",
      KafkaOffsetBackingStore.OFFSET_STORAGE_TOPIC_CONFIG -> "connect-offsets",
      KafkaConfigStorage.CONFIG_TOPIC_CONFIG -> "connect-configs",

      WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.storage.StringConverter",
      WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url" -> "http://192.168.99.100:8081/",

      WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.storage.StringConverter",
      WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG -> "io.confluent.connect.avro.AvroConverter",
      "internal.value.converter.schema.registry.url" -> "http://192.168.99.100:8081/"))

    //  "key.converter.schemas.enable"-> "false",
    //  "value.converter.schemas.enable" -> "false",
    //    "value.converter.schemas.enable" -> "false",
    //    WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000",
    //    "internal.key.converter.schemas.enable", "false",
    //    "internal.value.converter.schemas.enable", "false")

    val connectorProps: Properties = PropertiesHelper.create(Map(
      ConnectorConfig.NAME_CONFIG -> "audit-cassandra-target",
      ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.tuplejump.kafka.connect.cassandra.CassandraSink",
      SinkConnector.TOPICS_CONFIG -> "audit-save-to-cassandra"
    ))

    return new ConnectEmbedded(workerProps, connectorProps)
  }

  def GetKafkaStreamConfiguration(KAFKA_BROKERS_LIST: String): StreamsConfig = {
    new StreamsConfig(Map(
      StreamsConfig.JOB_ID_CONFIG -> "spark-audit-job",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.99.100:2181",
      StreamsConfig.ZOOKEEPER_CONNECT_CONFIG -> "192.168.99.100:2181",

      StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroSerializer],

      StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer])
    )
  }

  private def isSorEvent(record:GenericRecord) = {
    record
      .get("payload")
      .asInstanceOf[GenericRecord]
      .get("sorDataTransactions") != null
  }

}


