package com.codinoverse
package io


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

object CsvReaderAndWriter {


  /**
   * Creates a DataFrame from a CSV file with specified schema.
   *
   * @param path   The path to the CSV file.
   * @param schema The schema to be applied to the DataFrame.
   * @return A DataFrame read from the CSV file with the specified schema.
   */
  def creatingDataFrame(path: String, schema: StructType)(implicit  spark:SparkSession) = {
    spark
      .read
      .option("inferSchema", "true")
      .schema(schema)
      .csv(path)
  }

  /**
   * Writes a DataFrame to a CSV file.
   *
   * @param df   The DataFrame to be written.
   * @param path The path where the DataFrame will be saved as a CSV file.
   */
  def WriteDataFrame(df: DataFrame, path: String)(implicit  spark:SparkSession): Unit = {
    // Write DataFrame to CSV file
    df.coalesce(1).write
      .mode(SaveMode.Overwrite) // Overwrite existing files
      .option("header", "true") // Write column names as header
      .option("filename","output.csv")
      .csv(path) // Save DataFrame as CSV file at the specified path
  }

}
