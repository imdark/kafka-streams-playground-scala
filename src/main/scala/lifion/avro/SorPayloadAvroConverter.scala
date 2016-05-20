package com.lifion.avro

import com.lifion.consumer.{actionEnum, actorTypeEnum, attributeTypeEnum}
import com.lifion.event.Event
import com.lifion.sordb.SorDataTransaction
import org.apache.avro.generic._

import scala.collection.mutable.ListBuffer

object SorPayloadAvroConverter {
  def convert(record:GenericRecord):SorPayload = {
    var sorDataTransactions = new ListBuffer[SorDataTransaction]()
    val sorDataChangesIterator = record.get("sorDataTransactions").asInstanceOf[GenericData.Array[GenericRecord]].iterator()
    while(sorDataChangesIterator.hasNext()) {
      sorDataTransactions += convertSorDataTransaction(sorDataChangesIterator.next())
    }

    SorPayload(
      sorDataTransactions.toList
    )
  }

  private def convertSorDataTransaction(record:GenericRecord) = {
    SorDataTransaction(
      record.get("ownerId").toString,
      ownerTypeEnum withName record.get("ownerType").toString,
      actionEnum withName record.get("action").toString,
      record.get("attributeName").toString,
      attributeTypeEnum withName record.get("attributeType").toString,
      record.get("value").toString,
      record.get("rowId").toString,
      record.get("recordType").toString
    )
  }
}