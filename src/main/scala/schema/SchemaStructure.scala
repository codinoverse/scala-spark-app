package com.codinoverse
package schema

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SchemaStructure {

  val salesSchema = StructType(
    Seq(
      StructField("product_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("order_date", DateType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("source_order", StringType, nullable = false)
    )
  )

  val menuSchema = StructType(
    Seq(
      StructField("product_id",StringType,nullable = false),
      StructField("product_name",StringType,nullable = false),
      StructField("product_price",IntegerType,nullable = false)
    )
  )

}
