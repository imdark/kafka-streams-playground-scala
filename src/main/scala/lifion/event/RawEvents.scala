//package com.lifion.event
//
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import com.lifion.avro.{ConversionHelper, Entity}
//import org.apache.spark.streaming.dstream._
//import org.apache.avro.generic._
//import org.apache.kafka.streams.kstream.KStream
//
//case class RawEventsTable(
//  event_id: String,
//  raw_event: Array[Byte]
//)
//
//object RawEvents {
//  def convertToTable(event:(Array[Byte], GenericRecord)) = {
//    RawEventsTable(
//      event._2.get("id").toString,
//      event._1
//    )
//  }
////  def saveToCassandra(events:KStream[(Array[Byte], GenericRecord)]) = {
////    events
////      .map(convertToTable)
////      .saveToCassandra("audit", "raw_events")
////  }
//}