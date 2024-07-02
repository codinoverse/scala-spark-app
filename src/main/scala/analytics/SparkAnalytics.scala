package com.codinoverse
package analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkAnalytics {

  /**
   * Calculates the total amount invested by each customer.
   * Assumes the input DataFrame has columns: customerId, itemId, amount.
   *
   * @param salesDF The DataFrame containing sales data.
   * @param menuDf The DataFrame containing menu data
   * @return DataFrame with total amount invested by each customer.
   */

  def totalAmountInvestedbyEachCustomer(salesDf:DataFrame,menuDf:DataFrame)={
    salesDf
      .join(menuDf,"product_id")
      .groupBy("customer_id")
      .agg(sum("product_price"))

  }

  /** ******
   * Function to calculate the total amount spent on each food item.
   *
   * @param salesDf DataFrame containing sales data
   * @param menuDf  DataFrame containing menu data, which includes product_id and product_name.
   * @return DataFrame with total amount spent on each food item, grouped by  product_id and product_name and price.
   *         ****** */

  def totalAmountSpentOnEachProduct(salesDf:DataFrame,menuDf:DataFrame)={
    salesDf
      .join(menuDf, "product_id")
      .groupBy("product_id", "product_name")
      .agg(sum("product_price") as ("total_spent_on_food"))


  }

  /** ******
   * Function to calculate the total amount of sales in each month.
   *
   * @param saleswithdatedetailsDf DataFrame containing sales data with date details, including year, month, and date.
   * @param menudf                 DataFrame containing menu data, which includes product_id and product_name and price.
   * @return DataFrame with total amount of sales for each month.
   *         ****** */


  def totalAmountOfSalesInEachMonth(saleswithdatedetailsDf:DataFrame,menudf:DataFrame)={
    saleswithdatedetailsDf
      .join(menudf, "product_id")
      .groupBy("order_month")
      .agg(sum("product_price") as ("total_sales_in_each_month"))
      .orderBy("order_month")

  }

  /** ******
   * Function to calculate the total amount of sales in each Year.
   *
   * @param saleswithdatedetailsDf DataFrame containing sales data with date details, including year, month, and date.
   * @param menudf                 DataFrame containing menu data, which includes product_id and product_name and price.
   * @return DataFrame with total amount of sales for each Year.
   *         ****** */

  def totalAmountOfSalesInEachYear(saleswithdatedetailsDf:DataFrame,menudf:DataFrame)={
    saleswithdatedetailsDf
      .join(menudf, "product_id")
      .groupBy("order_year")
      .agg(sum("product_price") as ("total_sales_in_each_year"))
      .orderBy("order_year")
  }


  /** ******
   * Function to calculate the total amount of sales in each quarter.
   *
   * @param saleswithdatedetailsDf DataFrame containing sales data with date details, including year, month, and date.
   * @param menudf                 DataFrame containing menu data, which includes  product_id and product_name and price.
   * @return DataFrame with total amount of sales for each quarter.
   *         ****** */

  def totalAmountOfSalesInEachQuarter(saleswithdatedetailsDf:DataFrame,menudf:DataFrame)={

    saleswithdatedetailsDf
      .join(menudf, "product_id")
      .groupBy("order_quarter")
      .agg(sum("product_price"))
  }

  /** ******
   * Function to calculate the total number of orders by each category.
   *
   * @param salesDf DataFrame containing sales data
   * @param menudf                 DataFrame containing menu data, which includes product_id and product_name and price.
   * @return DataFrame with the total number of orders for each Product.
   *         ****** */

  def totalNumberOfOrdersByEachProduct(salesDf: DataFrame, menudf: DataFrame) = {

    salesDf
      .join(menudf, "product_id")
      .groupBy("product_id", "product_name")
      .agg(count("product_id") as ("total_number_of_orders"))

  }

  /** ******
   * Function to get the top ordered products.
   *
   * @param number  The number of top ordered products to return.
   * @param salesDf DataFrame containing sales data .
   * @param menudf  DataFrame containing menu data, which includes product_id and product_name and price.
   * @return DataFrame with the top ordered products limited to the specified number.
   *         ****** */

  def topOrderedProducts(number:Int,salesDf: DataFrame, menudf: DataFrame)={
    totalNumberOfOrdersByEachProduct(salesDf,menudf).orderBy(desc("total_number_of_orders"))
      .limit(number)
  }

  /** ******
   * Function to get the most ordered product.
   *
   * @param salesdf DataFrame containing sales data.
   * @param menudf  DataFrame containing menu data,which includes product_id and product_name and price.
   * @return DataFrame with the most ordered product.
   *         ****** */

  def mostOrderdProduct(salesdf:DataFrame,menudf:DataFrame)={
    topOrderedProducts(1,salesdf,menudf)
  }

  /** ******
   * Function to calculate the visit frequency of customers from a specific order source.
   *
   * @param salesdf DataFrame containing sales data .
   * @param source  String representing the order source to filter by.
   * @return DataFrame with the visit frequency of customers from the specified order source.
   *         ****** */


  def customerVisitfrequency(salesdf:DataFrame,source:String)={
    salesdf
      .filter(s"source_order=='${source}'")
      .groupBy("customer_id")
      .agg(countDistinct("order_date") as("Total_Visits"))
  }

  /** ******
   * Function to calculate the total sales amount for each country.
   *
   * @param salesDf DataFrame containing sales data.
   * @param menuDf  DataFrame containing menu data, includes product_id and product_name and price.
   * @return DataFrame with the total sales amount for each country.
   *         ****** */

  def totalSalesByEachCountry(salesDf:DataFrame,menuDf:DataFrame)={
    salesDf
      .join(menuDf,"product_id")
      .groupBy("location")
      .agg(sum("product_price") as("Total Amount"))
  }

  /** ******
   * Function to calculate the total sales amount for each source order.
   *
   * @param salesDf DataFrame containing sales data.
   * @param menuDf  DataFrame containing menu data, which includes product_id and product_name and price.
   * @return DataFrame with the total sales amount for each source order.
   *         ****** */

  def totalSalesBySourceOrder(salesDf:DataFrame,menuDf:DataFrame)={

    salesDf
      .join(menuDf, "product_id")
      .groupBy("source_order")
      .agg(sum("product_price"))
  }

}
