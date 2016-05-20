//package com.lifion.event
//
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import com.lifion.avro.{ConversionHelper, Entity}
//import com.lifion.avro.EventAvroConverter
//import org.apache.avro.generic._
//import org.apache.kafka.streams.kstream.KStream
//
//case class EventsByEntityTable(
//  entity: UDTValue,
//  created_at:String,
//  transaction_id: String,
//  event_id: String,
//  user_id: String,
//  topic_code: String,
//  miniapp_id: String,
//  organization_id: String,
//  entities: Seq[UDTValue]
//)
//
//object EventsByEntity {
//  def convertToTable(event:Event, entity:Entity) = {
//    EventsByEntityTable(
//      ConversionHelper.convertEntity(entity),
//      event.timeStamp,
//      event.transactionId,
//      event.id,
//      event.userId,
//      event.topicCode,
//      event.miniappId,
//      event.organizationId,
//      ConversionHelper.convertEntities(event)
//    )
//  }
//  def saveToCassandra(events:KStream[(Array[Byte], GenericRecord)]) = {
//    events
//      .mapValues((v) => EventAvroConverter.convert(v))
//      .filter((k,v)=>  false)
////      .flatMapValues((event) => {
//////        event.entities.map((entity) => convertToTable(event, entity))
////      })
////      .saveToCassandra("audit", "events_by_entity")
//  }
//}