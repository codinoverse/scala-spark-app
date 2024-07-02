# Codinoverse Catalogue App

This is a Scala Spark application that performs various data analysis tasks on sales and menu data. The application loads the data from CSV files, performs data enrichment, and generates several analytics reports.

## Features

This application provides the following functionalities:

1. **Load Sales and Menu Data**: The application loads the sales and menu data from CSV files located in the `src/main/resources/data` directory.
2. **Enrich Sales Data**: The application adds additional columns to the sales data, such as `order_year`, `order_month`, and `order_quarter`.
3. **Calculate Total Amount Invested by Each Customer**: The application calculates the total amount invested by each customer and writes the results to the `output/totalAmountByCustomerDf` directory.
4. **Calculate Total Amount Spent on Each Product**: The application calculates the total amount spent on each product (food) and writes the results to the `output/totalAmountOnFoodDf` directory.
5. **Calculate Total Amount of Sales in Each Month**: The application calculates the total amount of sales in each month and writes the results to the `output/amountOfSalesinMonthDf` directory.
6. **Calculate Total Amount of Sales in Each Year**: The application calculates the total amount of sales in each year and writes the results to the `output/amountOfSalesinYearDf` directory.
7. **Calculate Total Amount of Sales in Each Quarter**: The application calculates the total amount of sales in each quarter and writes the results to the `output/amountOfSalesinQuarterDf` directory.
8. **Calculate Total Number of Orders by Each Product**: The application calculates the total number of orders by each product and writes the results to the `output/totalNumberOfOrdersByCategory` directory.
9. **Get the Top 10 Ordered Products**: The application retrieves the top 10 ordered products and writes the results to the `output/topOrderedProductsDf` directory.
10. **Get the Most Ordered Product**: The application identifies the most ordered product and writes the results to the `output/mostOrderedProduct` directory.
11. **Calculate Customer Visit Frequency for the "Restaurant" Source**: The application calculates the customer visit frequency for the "Restaurant" source and writes the results to the `output/customerRestaurantFrequncyDf` directory.
12. **Calculate Total Sales by Each Country**: The application calculates the total sales by each country and writes the results to the `output/salesByEachCountryDf` directory.
13. **Calculate Total Sales by Source Order**: The application calculates the total sales by source order and writes the results to the `output/salesBySourceOrderDf` directory.

## Prerequisites

- Scala: 2.12.15
- Apache Spark: 3.2.0
- Apache Log4j 2.x

## Usage

1. Clone the repository.
2. Navigate to the directory.
3. Run `sbt run`.

## How to Run in Standalone Mode in Windows

1. Run `sbt clean && sbt assembly` to get the jar file.
2. Start the master:
    - Open Command Prompt as Administrator.
    - Navigate to the Spark installed folder.
    - Run this command: `bin\spark-class2.cmd org.apache.spark.deploy.master.Master`
3. Start the worker:
    - Open another Command Prompt as Administrator.
    - Navigate to the Spark installed folder.
    - Run this command: `bin\spark-class2.cmd org.apache.spark.deploy.worker.Worker -c 1 -m 4G <master-url>`
4. Open Command Prompt and run the `spark-submit` command:
    ```sh
    C:\Spark\spark-3.3.1-bin-hadoop2\bin\spark-submit --class <mainClass> --master <master-url> <jarfile>
    ```
   Example:
    ```sh
    C:\Spark\spark-3.3.1-bin-hadoop2\bin\spark-submit --class com.india.Driver --master spark://192.121.11.111:7077 basicapp_2.13-0.1.0-SNAPSHOT.jar
    ```
