package bdms
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object SuperStore {

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SuperStore_sales").getOrCreate()
  val user = "root"
  val pwd = "1234567890"

  def get_table(table: String): DataFrame = {
    val DF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/vegetable_db")
      .option("driver", "com.mysql.cj.jdbc.Driver") // updated driver class
      .option("dbtable", table)
      .option("user", user)
      .option("password", pwd).load()
    DF
  }


  // 1.1
  def checkNulls(df: DataFrame, columnName: String): Unit = {
    val nullCount = df.filter(col(columnName).isNull).count()

    if (nullCount > 0) {
      println(s"Column '$columnName' has $nullCount null values.")
    } else {
      println(s"Column '$columnName' has no null values.")
    }
  }

  // 1.2 'Category Name' count
  def categoryCount(dfSales: DataFrame, dfItems: DataFrame): Unit = {
    val categoryCount = dfSales
      .join(dfItems, Seq("Item_Code"))
      .groupBy("Category_Name")
      .count()
    categoryCount.show()
  }

  // 1.3 Function for Total Sales and Profit/Loss Yearly Wise

  def totalSalesAndProfitLossYearly(dfSales: DataFrame, dfWholesale: DataFrame, itemCode: Long): Unit = {
    val salesAlias = dfSales.alias("sales")
    val wholesaleAlias = dfWholesale.alias("wholesale")

    val resultDF = salesAlias
      .join(wholesaleAlias, salesAlias("Item_Code") === wholesaleAlias("Item_Code"))
      .filter(salesAlias("Item_Code") === itemCode)
      .withColumn("Year", year(col("sales.Date")))
      .groupBy("Year")
      .agg(
        format_number(sum(salesAlias("Quantity_Sold")), 3).alias("Total_Quantity"),
        format_number(sum((salesAlias("Unit_Selling_Price") - wholesaleAlias("Wholesale_Price")) * salesAlias("Quantity_Sold")), 3).alias("Profit_Loss")
      )
    resultDF.show()
  }

  // 1.4 Evaluate the Effectiveness of Discount Strategies
  def evaluateDiscountEffectiveness(dfSales: DataFrame): Unit = {
    val discountEffectiveness = dfSales.groupBy("Discount").agg(avg("Quantity_Sold").alias("Average_Quantity(kg)"))
    discountEffectiveness.show()
  }


  // 2.1 method to check for duplications
  def checkDuplicates(df: DataFrame, columnName: String): Unit = {
    val duplicateCount = df.groupBy(columnName).count().filter("count > 1").count()

    if (duplicateCount > 0) {
      println(s"Column '$columnName' has $duplicateCount duplicate values.")
    } else {
      println(s"Column '$columnName' has no duplicate values.")
    }
  }

  // 2.2 Total Yearly quantity Sales
  def totalYearlySales(dfSales: DataFrame): Unit = {
    val yearlySales = dfSales
      .withColumn("Year", year(col("Date")))
      .groupBy("Year")
      .agg(
        format_number(sum("Quantity_Sold"),3).alias("Total_Quantity(kg)"),
        format_number(sum("Unit_Selling_Price"),2).alias("Total_revenue")
      )
    yearlySales.show()

  }

  // 2.3 Function for Total Sales and Profit/Loss Monthly Wise for a specific 'Item_Code'
  def totalSalesAndProfitLossMonthly(dfSales: DataFrame, dfWholesale: DataFrame, itemCode: Long): Unit = {
    val salesAlias = dfSales.alias("sales")
    val wholesaleAlias = dfWholesale.alias("wholesale")

    val resultDF = salesAlias
      .join(wholesaleAlias, salesAlias("Item_Code") === wholesaleAlias("Item_Code"))
      .filter(salesAlias("Item_Code") === itemCode)
      .withColumn("Month", month(col("sales.Date")))
      .groupBy("Month")
      .agg(
        format_number(sum(salesAlias("Quantity_Sold")), 3).alias("Total_Quantity"),
        format_number(sum((salesAlias("Unit_Selling_Price") - wholesaleAlias("Wholesale_Price")) * salesAlias("Quantity_Sold")), 3).alias("Profit_Loss")
      )

    resultDF.show()
  }

  // 2.4 Count 'Sale or Return'
  def countSaleOrReturn(dfSales: DataFrame): Unit = {
    val saleOrReturnCount = dfSales.groupBy("Sale_or_Return").count()
    saleOrReturnCount.show()
  }




  // 3.1 Top 10 'Item Name'
  def topItems(dfSales: DataFrame, dfItems: DataFrame): Unit = {
    val topItems = dfSales
      .join(dfItems, Seq("Item_Code"))
      .groupBy("Item_Name")
      .agg(sum("Quantity_Sold").alias("Total_Quantity(kg)"))
      .orderBy(desc("Total_Quantity(kg)"))
      .limit(10)
    topItems.show()
  }

  // 3.2 Total Sales Revenue
  def totalSalesRevenue(dfSales: DataFrame): Unit = {
    val totalRevenue = dfSales
      .withColumn("Total_Revenue", col("Quantity_Sold") * col("Unit_Selling_Price"))
      .orderBy(desc("Total_Revenue"))
    totalRevenue.show()
  }

  // 3.3 Add a 'Day' Column to dfSales
  def addDayColumn(dfSales: DataFrame): DataFrame = {
    dfSales.withColumn("Day", date_format(col("Date"), "EEE"))
  }

  // 3.4 Function to calculate total profit/loss day wise
  def totalProfitLossDayWise(dfSales: DataFrame, dfWholesale: DataFrame): Unit = {
    val salesAlias = dfSales.alias("sales")
    val wholesaleAlias = dfWholesale.alias("wholesale")

    val resultDF = salesAlias
      .join(wholesaleAlias, col("sales.Item_Code") === col("wholesale.Item_Code"))
      .withColumn("Day", dayofweek(col("sales.Date")))
      .groupBy("Day")
      .agg(
        sum((col("sales.Unit_Selling_Price") - col("wholesale.Wholesale_Price")) * col("sales.Quantity_Sold")).alias("Profit_Loss")
      )


    resultDF.show()
  }

  // 4.1 get Item details
  def item_details(df: DataFrame, code: Long): Unit = {
    val details = df.filter(col("Item_Code") === code)
      .select("Item_Name", "Category_Code", "Category_Name")
    details.show()
  }

  // 4.2 Count 'Discount'
  def countDiscount(dfSales: DataFrame): Unit = {
    val discountCount = dfSales.groupBy("Discount").count()
    discountCount.show()
  }


  // 4.3 Sales Time Analysis: Identify peak sales hours in a day
  def peakSalesHours(dfSales: DataFrame): Unit = {
    val resultDF = dfSales
      .withColumn("Hour", hour(col("Time")))
      .groupBy("Hour")
      .agg(count("sale_id").alias("SalesCount"))
      .orderBy(col("SalesCount").desc)

    resultDF.show()
  }


  def main(args: Array[String]): Unit={
    val df_items = get_table("item_table")
    val df_sales = get_table("sales_table")
    val df_wholesale = get_table("whole_sale")
    val df_lossrate = get_table("loss")


    // Call the methods as needed

    // 1.1
    println("Null checking method: ")
    checkNulls(df_items, "Item_Code")
    // 1.2
    println("Total category count method: ")
    categoryCount(df_sales, df_items)
    // 1.3
    println("total Sales And Profit_Loss Yearly ")
    totalSalesAndProfitLossYearly(df_sales, df_wholesale, 102900005115762L)
    // 1.4
    println("evaluate Discount Effectiveness ")
    evaluateDiscountEffectiveness(df_sales)



    // 2.1
    println("Duplication checking method: ")
    checkDuplicates(df_items, "Item_Code")
    // 2.2
    println("Total Yearly sales method: ")
    totalYearlySales(df_sales)
    // 2.3
    println("total Sales And Profit_Loss Monthly ")
    totalSalesAndProfitLossMonthly(df_sales, df_wholesale, 102900005115762L)
    // 2.4
    println("Total Discount and Non Discount items:")
    countDiscount(df_sales)


    // 3.1
    item_details(df_items,102900005115762L)
    // 3.2
    println("Total Sales Revenue method: ")
    totalSalesRevenue(df_sales)
    // 3.3
    println("Creating a DF with day column:")
    val dfSalesWithDays = addDayColumn(df_sales)
    dfSalesWithDays.show()
    // 3.4
    println("total Sales And Profit_Loss Day wise: ")
    totalProfitLossDayWise(df_sales, df_wholesale)


    // 4.1
    println("Top sold Item checking method: ")
    topItems(df_sales, df_items)
    // 4.2
    println("Total Sell or Return items: ")
    countSaleOrReturn(df_sales)
    // 4.3
    println("peak Sales Hours:")
    peakSalesHours(df_sales)

  }

}
