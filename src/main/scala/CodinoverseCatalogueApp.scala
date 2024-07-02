package com.codinoverse

import com.codinoverse.analytics.SparkAnalytics
import com.codinoverse.io.CsvReaderAndWriter
import com.codinoverse.schema.SchemaStructure
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, quarter, year}


object CodinoverseCatalogueApp extends App{

  System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.xml")

  val logger = org.apache.logging.log4j.LogManager.getLogger("com.codinoverse")



  // Create a SparkSession with necessary configurations
  implicit val spark = SparkSession
    .builder()
    .appName("scala-spark-app")
    //.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
    .master("local[*]")
    .getOrCreate()


  logger.info(s"Loading the sales and menu Csv files")

  // Load sales and menu data from CSV files
  val salesDf = CsvReaderAndWriter.creatingDataFrame("src/main/resources/data/sales.csv",SchemaStructure.salesSchema)
  val menuDf = CsvReaderAndWriter.creatingDataFrame("src/main/resources/data/menu.csv",SchemaStructure.menuSchema)

  // Enrich the sales data with additional columns
  val salesEnrichedDf = salesDf
    .withColumn("order_year",year(col("order_date")))
    .withColumn("order_month",month(col("order_date")))
    .withColumn("order_quarter",quarter(col("order_date")))


  // Calculate the total amount invested by each customer
  val totalAmountByCustomerDf = SparkAnalytics
    .totalAmountInvestedbyEachCustomer(salesDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(totalAmountByCustomerDf,"output/totalAmountByCustomerDf")

  // Calculate the total amount spent on each Product(food)
  val totalAmountOnProductDf = SparkAnalytics
    .totalAmountSpentOnEachProduct(salesDf, menuDf)
  CsvReaderAndWriter.WriteDataFrame(totalAmountOnProductDf,"output/totalAmountOnFoodDf")

  // Calculate the total amount of sales in each month
  val amountOfSalesinMonthDf = SparkAnalytics
    .totalAmountOfSalesInEachMonth(salesEnrichedDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(amountOfSalesinMonthDf,"output/amountOfSalesinMonthDf")

  // Calculate the total amount of sales in each year
  val amountOfSalesinYearDf = SparkAnalytics
    .totalAmountOfSalesInEachYear(salesEnrichedDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(amountOfSalesinYearDf,"output/amountOfSalesinYearDf")

  // Calculate the total amount of sales in each quarter
  val amountOfSalesinQuarterDf = SparkAnalytics
    .totalAmountOfSalesInEachQuarter(salesEnrichedDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(amountOfSalesinQuarterDf,"output/amountOfSalesinQuarterDf")

  // Calculate the total number of orders by each product
  val numberOfOrdersByEachproductDf = SparkAnalytics
    .totalNumberOfOrdersByEachProduct(salesDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(numberOfOrdersByEachproductDf,"output/totalNumberOfOrdersByCategory")

  // Get the top 10 ordered products
  val topOrderedProductsDf = SparkAnalytics
    .topOrderedProducts(10,salesDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(topOrderedProductsDf,"output/topOrderedProductsDf")

  // Get the most ordered product
  val mostOrderedProduct = SparkAnalytics
    .mostOrderdProduct(salesDf,menuDf)
  CsvReaderAndWriter.WriteDataFrame(mostOrderedProduct,"output/mostOrderedProduct")

  // Calculate the customer visit frequency for the "Restaurant" source
  val customerRestaurantFrequncyDf = SparkAnalytics
    .customerVisitfrequency(salesDf,"Restaurant")
  CsvReaderAndWriter.WriteDataFrame(mostOrderedProduct,"output/customerRestaurantFrequncyDf")

  // Calculate the total sales by each country
  val salesByEachCountryDf = SparkAnalytics
    .totalSalesByEachCountry(salesDf, menuDf)
  CsvReaderAndWriter.WriteDataFrame(salesByEachCountryDf,"output/salesByEachCountryDf")

  // Calculate the total sales by source order
  val salesBySourceOrderDf = SparkAnalytics
    .totalSalesBySourceOrder(salesDf, menuDf)
  CsvReaderAndWriter.WriteDataFrame(salesBySourceOrderDf,"output/salesBySourceOrderDf")

  logger.info("Spark job completed successfully")

  spark.stop()


}
