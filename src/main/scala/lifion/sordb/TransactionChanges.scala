package com.lifion.sordb

import com.datastax.spark.connector.UDTValue
import com.lifion.avro._
import com.lifion.event.Event
import org.apache.avro.generic._
import org.apache.kafka.streams.kstream.KStream

case class TransactionChangesTable(
  transaction_id: String,
  event_id: String,
  change_index: Int,
  created_at:String,
  user_id: String,
  topic_code: String,
  actor: UDTValue,
  entities: Seq[UDTValue],
  owner: UDTValue,
  record: UDTValue,
  change: UDTValue
)

object TransactionChanges {
  def convertToTable(event:Event, change:SorDataTransaction, index:Int) = {
    TransactionChangesTable(
      event.transactionId,
      event.id,
      index,
      event.timeStamp,
      event.userId,
      event.topicCode,
      convertActor(event),
      ConversionHelper.convertEntities(event),
      convertOwner(change),
      convertRecord(change),
      convertChange(change)
    )
  }

  private def convertChange(change:SorDataTransaction) = {
    UDTValue.fromMap(Map(
      "action" -> change.action,
      "attribute_name" -> change.attributeName,
      "attribute_type" -> change.attributeType,
      "attribute_value" -> change.value
    ))
  }

  private def convertActor(event:Event) = {
    UDTValue.fromMap(Map(
      "actor_id" -> event.actorId,
      "actor_type" -> event.actorType
    ))
  }

  private def convertOwner(change:SorDataTransaction) = {
    UDTValue.fromMap(Map(
      "owner_id" -> change.ownerId,
      "owner_type" -> change.ownerType
    ))
  }

  private def convertRecord(change:SorDataTransaction) = {
    UDTValue.fromMap(Map(
      "record_id" -> change.rowId,
      "record_type" -> change.recordType
    ))
  }

  private def isSorEvent(record:GenericRecord) = {
    record
      .get("payload")
      .asInstanceOf[GenericRecord]
      .get("sorDataTransactions") != null
  }
//
//  def saveToCassandra(events:KStream[(Array[Byte], GenericRecord)]) = {
//    events
//      .filter((k,v) => isSorEvent(v))
//      .map((record) => EventAvroConverter.convert(record))
//      .flatMap((event) => {
//        var index = -1
//        val sorPayload = SorPayloadAvroConverter.convert(event.payload)
//        sorPayload.sorDataTransactions.map((change) => {
//          index += 1
//          convertToTable(event, change, index)
//        })
//      })
//      .saveToCassandra("audit", "transaction_changes")
//  }
}