package com.codinoverse

import com.codinoverse.analytics.SparkAnalytics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.codinoverse.io.CsvReaderAndWriter
import com.codinoverse.schema.SchemaStructure
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class CodinoverseCatalogueAppTest extends FunSuite with BeforeAndAfterAll{

  @transient implicit var spark:SparkSession=_


  var menuTestdf: DataFrame = _
  var salesTestdf: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("CodinoverseCatalogueAppTest")
      .master("local[*]")
      .getOrCreate()

    menuTestdf = CsvReaderAndWriter.creatingDataFrame("src/test/resources/data/testmenu.csv",SchemaStructure.menuSchema)
    salesTestdf = CsvReaderAndWriter.creatingDataFrame("src/test/resources/data/testsales.csv",SchemaStructure.salesSchema)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Sales Data file loading") {
    val salesTestdf = CsvReaderAndWriter
      .creatingDataFrame("src/main/resources/data/sales.csv", SchemaStructure.salesSchema)

    val recordCount = salesTestdf.count()
    assert(recordCount==344,"record count should be 344")

  }

  test("Menu data file loading") {
    val menuTestdf =CsvReaderAndWriter
      .creatingDataFrame("src/main/resources/data/menu.csv",SchemaStructure.menuSchema)

    val recordCount = menuTestdf.count()
    assert(recordCount==20,"record count should be 20")
  }

  test("amountSpentByEachCustomer") {

    val resultDf = SparkAnalytics.totalAmountInvestedbyEachCustomer(salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("customer_id", StringType, nullable = false)
      .add("sum_product_price", IntegerType, nullable = false)
    val data = Seq(
      Row("F", 230),
      Row("E", 270),
      Row("B", 220),
      Row("D", 200),
      Row("C", 420),
      Row("A", 100),
      Row("G", 110)
    )
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertDataFrameEquals(resultDf,expectedDf)

  }

  test("amountSpentOnEachProduct") {
    val resultDf = SparkAnalytics.totalAmountSpentOnEachProduct(salesTestdf, menuTestdf)
    val schema = new StructType()
      .add("product_id", StringType, nullable = false)
      .add("product_name", StringType, nullable = false)
      .add("total_spent_on_food", IntegerType, nullable = false)

    val data = Seq(
      Row("4", "Dosa", 330),
      Row("6", "Pasta", 180),
      Row("7", "Idly", 180),
      Row("2", "Chowmin", 300),
      Row("1", "PIZZA", 200),
      Row("3", "sandwich", 360)
    )

    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertDataFrameEquals(resultDf,expectedDf)
  }

  test("amountOfSalesInEachMonth"){
    val enrichedDf = salesTestdf
      .withColumn("order_month",month(col("order_date")))

    val resultDf = SparkAnalytics
      .totalAmountOfSalesInEachMonth(enrichedDf,menuTestdf)

    val schema = new StructType()
      .add("order_month", IntegerType, nullable = false)
      .add("total_sales_in_each_month", IntegerType, nullable = false)

    val data = Seq(
      Row(1, 150),
      Row(2, 120),
      Row(4, 90),
      Row(5, 180),
      Row(6, 460),
      Row(7, 110),
      Row(8, 110),
      Row(10, 210),
      Row(12, 120)
    )

    val expecteddf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
   assertDataFrameEquals(resultDf,expecteddf)

  }

  test("amountOfSalesinEachYear"){

    val enricheDf = salesTestdf
      .withColumn("order_year",year(col("order_date")))

    val resultDf = SparkAnalytics.totalAmountOfSalesInEachYear(enricheDf,menuTestdf)

    val schema = new StructType()
      .add("order_year", IntegerType, nullable = false)
      .add("total_sales_in_each_year", IntegerType, nullable = false)

    val data = Seq(
      Row(2023, 1280),
      Row(2024, 270)
    )

    val expecteddf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertDataFrameEquals(resultDf,expecteddf)

  }


  test("Amount Of sales in each quarter"){
    val enrichedDf = salesTestdf
      .withColumn("order_quarter",quarter(col("order_date")))

    val resultsDf = SparkAnalytics.totalAmountOfSalesInEachQuarter(enrichedDf,menuTestdf)
    val schema = new StructType()
      .add("order_quarter", IntegerType, nullable = false)
      .add("sum_product_price", IntegerType, nullable = false)

    val data = Seq(
      Row(1, 270),
      Row(3, 220),
      Row(4, 330),
      Row(2, 730)
    )

    val expecteDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertDataFrameEquals(resultsDf,expecteDf)

  }

  test("Number of Orders by each product"){
    val resultDf = SparkAnalytics.totalNumberOfOrdersByEachProduct(salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("product_id", StringType, nullable = false)
      .add("product_name", StringType, nullable = false)
      .add("total_number_of_orders", IntegerType, nullable = false)
    val data = Seq(
      Row("4", "Dosa", 3),
      Row("6", "Pasta", 1),
      Row("7", "Idly", 2),
      Row("2", "Chowmin", 2),
      Row("1", "PIZZA", 2),
      Row("3", "sandwich", 3)
    )
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    assertDataFrameEquals(resultDf,expectedDf)

  }

  test("Top Ordered products"){
    val resultsDf = SparkAnalytics.topOrderedProducts(5,salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("product_id", StringType, nullable = false)
      .add("product_name", StringType, nullable = false)
      .add("total_number_of_orders", IntegerType, nullable = false)

    val data = Seq(
      Row("4", "Dosa", 3),
      Row("3", "sandwich", 3),
      Row("7", "Idly", 2),
      Row("2", "Chowmin", 2),
      Row("1", "PIZZA", 2)
    )

    val expecteDf = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    assertDataFrameEquals(resultsDf,expecteDf)


  }

  test("Most Ordered Product") {

    val resultsDf = SparkAnalytics.mostOrderdProduct(salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("product_id",StringType,nullable=false)
      .add("product_name",StringType,nullable = false)
      .add("total_number_of_orders",IntegerType,nullable = false)

    val data = Seq(
      Row("4","Dosa",3)
    )

    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    assertDataFrameEquals(resultsDf,expectedDf)
  }

  test("Customer Restaurant Frequency") {
    val resultsDf = SparkAnalytics.customerVisitfrequency(salesTestdf,"Restaurant")

    val schema = new StructType()
      .add("customer_id",StringType,nullable = false)
      .add("Total_Visits",IntegerType,nullable = false)

    val data = Seq(
      Row("B",1)
    )
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    assertDataFrameEquals(resultsDf,expectedDf)
  }

  test("Sales By each Country"){
    val resultsDf = SparkAnalytics.totalSalesByEachCountry(salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("location", StringType, nullable = false)
      .add("Total Amount", IntegerType, nullable = false)

    val data = Seq(
      Row("Sri Lanka", 120),
      Row("India", 550),
      Row("USA", 460),
      Row("UK", 420)
    )
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    assertDataFrameEquals(resultsDf,expectedDf)
  }

  test("Sales By each Source_Order"){
    val resultsDf = SparkAnalytics.totalSalesBySourceOrder(salesTestdf,menuTestdf)
    val schema = new StructType()
      .add("source_order", StringType, nullable = false)
      .add("sum(product_price)", IntegerType, nullable = false)

    val data = Seq(
      Row("Swiggy", 900),
      Row("UberEats", 210),
      Row("Restaurant", 120),
      Row("FoodPanda", 320)
    )

    val expecteddf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertDataFrameEquals(resultsDf,expecteddf)
  }




  private def assertDataFrameEquals(df1:DataFrame,df2:DataFrame):Unit={
    assert(df1.collect().toSet==df2.collect().toSet)
  }





}
