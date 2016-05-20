//package com.lifion.event
//
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import com.lifion.avro.ConversionHelper
//import org.apache.spark.streaming.dstream._
//import com.lifion.avro.EventAvroConverter
//import org.apache.avro.generic._
//import org.apache.kafka.streams.kstream.KStream
//
//case class EventsByMiniappTable(
//  miniapp_id: String,
//  organization_id: String,
//  created_at: String,
//  transaction_id: String,
//  event_id: String,
//  user_id: String,
//  entities: Seq[UDTValue],
//  topic_code: String
//)
//
//object EventsByMiniapp {
//  def convertToTable(event:Event) = {
//    EventsByMiniappTable(
//      event.miniappId,
//      event.organizationId,
//      event.timeStamp,
//      event.transactionId,
//      event.id,
//      event.userId,
//      ConversionHelper.convertEntities(event),
//      event.topicCode
//    )
//  }
//  def saveToCassandra(events:KStream[(Array[Byte], GenericRecord)]) = {
//    events
//      .map(_._2)
//      .map(EventAvroConverter.convert(_))
//      .filter((event) => !event.miniappId.isEmpty())
//      .map(convertToTable(_))
//      .saveToCassandra("audit", "events_by_miniapp")
//  }
//}
//
