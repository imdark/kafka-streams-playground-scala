//package com.github.rollulus.myprocessor
//
//import java.util.Properties
//
//import com.fasterxml.jackson.databind.JsonNode
//import com.tuplejump.kafka.connect.cassandra.CassandraCluster
//import io.confluent.kafka.serializers.KafkaAvroDeserializer
//import org.apache.kafka.common.serialization.{LongDeserializer, _}
//import org.apache.kafka.connect.api.ConnectEmbedded
//import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}
//import org.apache.kafka.connect.runtime.distributed.DistributedConfig
//import org.apache.kafka.connect.sink.SinkConnector
//import org.apache.kafka.connect.storage.{KafkaConfigStorage, KafkaOffsetBackingStore}
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
//import org.apache.kafka.streams.processor.{AbstractProcessor, Processor, ProcessorSupplier}
//import org.slf4j.{Logger, LoggerFactory}
//
//import collection.JavaConversions.mapAsJavaMap
//
//class AvroToCassandra {
//  private val log: Logger = LoggerFactory.getLogger(classOf[AvroToCassandra])
//  private val DEFAULT_BOOTSTRAP_SERVERS: String = "localhost:9092"
//
//  @throws[Exception]
//  def main(args: Array[String]) {
//    val bootstrapServers: String = if (args.length == 1) args(0)
//    else DEFAULT_BOOTSTRAP_SERVERS
//    val connect: ConnectEmbedded = createWikipediaFeedConnectInstance(bootstrapServers)
//    connect.start()
//
//    val streams: KafkaStreams = createWikipediaStreamsInstance(bootstrapServers)
//    try {
//      streams.start()
//    }
//    catch {
//      case e: Throwable =>
//        log.error("Stopping the application due to streams initialization error ", e)
//        connect.stop()
//    }
//    Runtime.getRuntime.addShutdownHook(new Thread(() => connect.stop()))
//
//    try {
//      connect.awaitStop()
//      log.info("Connect closed cleanly...")
//    } finally {
//      streams.close()
//      log.info("Streams closed cleanly...")
//    }
//  }
//
//  @throws[Exception]
//  private def createCassandraConnector(cassandraBootstrapServers: String): ConnectEmbedded = {
//    val workerProps = PropertiesHelper.create(Map(
//    DistributedConfig.GROUP_ID_CONFIG -> "audit-connect",
//    WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> cassandraBootstrapServers,
//    KafkaOffsetBackingStore.OFFSET_STORAGE_TOPIC_CONFIG -> "connect-offsets",
//    KafkaConfigStorage.CONFIG_TOPIC_CONFIG -> "connect-configs",
//    WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> classOf[org.apache.kafka.connect.storage.StringConverter],
//    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> classOf[io.confluent.connect.avro.AvroConverter]))
//
//    //  "key.converter.schemas.enable"-> "false",
////  "value.converter.schemas.enable" -> "false",
////    "value.converter.schemas.enable" -> "false",
////    WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000",
////    "internal.key.converter.schemas.enable", "false",
////    "internal.value.converter.schemas.enable", "false")
//
//    val connectorProps: Properties = PropertiesHelper.create(Map(
//      ConnectorConfig.NAME_CONFIG -> "audit-cassandra-target",
//      ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.tuplejump.kafka.connect.cassandra.CassandraSink",
//      SinkConnector.TOPICS_CONFIG -> "audit-save-to-cassandra"
//    ))
//
//    return new ConnectEmbedded(workerProps, connectorProps)
//  }
//
//  private def createWikipediaStreamsInstance(bootstrapServers: String): KafkaStreams = {
//    val builder: KStreamBuilder = new KStreamBuilder
//
//    val props: Properties = PropertiesHelper.create(Map(
//      StreamsConfig.JOB_ID_CONFIG -> "wikipedia-streams",
//      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
//      StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//      StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer]))
//
//    val wikipediaRaw: KStream[JsonNode, JsonNode] = builder.stream("wikipedia-raw")
////    val wikipediaParsed: KStream[String, WikipediaMessage] = wikipediaRaw.map(WikipediaMessage.parceIRC).filter(WikipediaMessage.filterNonNull).through("wikipedia-parsed", stringSerializer, wikiSerializer, stringDeserializer, wikiDeserializer)
////    val totalEditsByUser: KTable[String, Long] = wikipediaParsed.filter((key, value) -> value.type == WikipediaMessage.Type.EDIT).countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "wikipedia-edits-by-user")
////    totalEditsByUser.toStream.process(
////      () => new AbstractProcessor[String, Long] {
////        override def process(user: String, numEdits: Long) {
////          System.out.println("USER: " + user + " num.edits: " + numEdits)
////        }
////      })
//    new KafkaStreams(builder, props)
//  }
//}
