package com.lifion.sordb

import com.lifion.avro.ownerTypeEnum
import com.lifion.consumer.{actionEnum, attributeTypeEnum}

/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
case class SorDataTransaction(ownerId: String, ownerType: ownerTypeEnum.Value, action: actionEnum.Value, attributeName: String, attributeType: attributeTypeEnum.Value, value: String, rowId: String, recordType: String)