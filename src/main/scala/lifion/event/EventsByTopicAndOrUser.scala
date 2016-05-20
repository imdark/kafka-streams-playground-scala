//package com.lifion.event
//
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import com.lifion.avro.ConversionHelper
//import org.apache.spark.streaming.dstream._
//import com.lifion.avro.EventAvroConverter
//import org.apache.avro.generic._
//
//case class EventsByTopicAndOrUserTable(
//  composite_key: String,
//  topic_code: String,
//  user_id: String,
//  created_at: String,
//  transaction_id: String,
//  event_id: String,
//  entities: Seq[UDTValue],
//  miniapp_id: String,
//  organization_id: String
//)
//
//object EventsByTopicAndOrUser {
//  def convertToTable(event:Event) = {
//
//    // TODO: check if userId is a mandatory field
//    val compositeKeys = Array(
//      s"${event.topicCode}:${event.userId}",
//      s"${event.topicCode}:",
//      s":${event.userId}"
//    )
//
//    compositeKeys.map((compositeKey) => {
//      EventsByTopicAndOrUserTable(
//        compositeKey,
//        event.topicCode,
//        event.userId,
//        event.timeStamp,
//        event.transactionId,
//        event.id,
//        ConversionHelper.convertEntities(event),
//        event.miniappId,
//        event.organizationId
//      )
//    })
//  }
//
//  def saveToCassandra(events:DStream[(Array[Byte], GenericRecord)]) = {
//    events
//      .map(_._2)
//      .map(EventAvroConverter.convert(_))
//      .flatMap(convertToTable(_))
//      .saveToCassandra("audit", "events_by_topic_code_andor_user_id")
//  }
//}
