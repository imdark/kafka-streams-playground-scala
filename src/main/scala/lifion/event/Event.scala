package com.lifion.event

import com.lifion.avro._
import com.lifion.consumer.actorTypeEnum
import org.apache.avro.generic._

/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
case class Event(id: String, organizationId: String, miniappId: String, transactionId: String, userId: String, actorType: actorTypeEnum.Value, actorId: String, topicCode: String, timeStamp: String, effectiveTimeStamp: String, entities: List[Entity], payload: GenericRecord)