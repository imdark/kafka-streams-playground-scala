package com.lifion.avro

//import com.datastax.driver.core.UDTValue
import com.datastax.spark.connector.UDTValue
import com.lifion.event.Event
object ConversionHelper {
  def convertEntities(event:Event) = {
    event.entities.map(convertEntity(_))
  }
  
  def convertEntity(entity:Entity) = {
    UDTValue.fromMap(Map(
      "entity_id" -> entity.entityId,
      "entity_type" -> entity.entityType
    ))
  }
}
