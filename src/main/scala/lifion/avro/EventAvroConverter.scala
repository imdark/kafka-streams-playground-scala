package com.lifion.avro

import com.lifion.consumer.{actionEnum, actorTypeEnum, attributeTypeEnum}
import com.lifion.event.Event
import com.lifion.sordb.SorDataTransaction
import org.apache.avro.generic._

import scala.collection.mutable.ListBuffer

object EventAvroConverter {
  def convert(record: GenericRecord):Event = {
    Event(
      record.get("id").toString,
      record.get("organizationId").toString,
      record.get("miniappId").toString,
      record.get("transactionId").toString,
      record.get("userId").toString,
      actorTypeEnum withName record.get("actorType").toString,
      record.get("actorId").toString,
      record.get("topicCode").toString,
      record.get("timeStamp").toString,
      record.get("effectiveTimeStamp").toString,
      convertEntities(record.get("entities").asInstanceOf[GenericData.Array[GenericRecord]]),
      record.get("payload").asInstanceOf[GenericRecord]
    )
  }

  private def convertEntities(recordArray:GenericData.Array[GenericRecord]) = {
    var entities = new ListBuffer[Entity]()
    val entityIterator = recordArray.iterator()
    while(entityIterator.hasNext()) {
      val entityRecord = entityIterator.next()
      entities += Entity(
        entityRecord.get("entityId").toString,
        entityRecord.get("entityType").toString
      )
    }
    entities.toList
  }
}
