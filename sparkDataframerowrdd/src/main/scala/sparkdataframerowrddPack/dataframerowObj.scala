package sparkdataframerowrddPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._ //_ means takes every thing under types

object dataframerowObj {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val data=sc.textFile("file:///C:/data/txns")
		val mapsplit=data.map(x=>x.split(","))
		val rowrdd=mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
		val structschema = StructType(Array(
		    StructField("txnno",StringType,true),
		    StructField("txndate",StringType,true),
		    StructField("custno",StringType,true),
		    StructField("amount",StringType,true),
		    StructField("category",StringType,true),
		    StructField("product",StringType,true),
		    StructField("city",StringType,true),
		    StructField("state",StringType,true),
		    StructField("spendby",StringType,true)))
		    
		    val df =spark.read.schema(structschema).format("csv").option("inferschema","true").load("file:///C:/data/txns")
		df.show()
		println("----partitioned data----")
		df.write.format("csv").partitionBy("category","spendby").mode("error").save("file:////E:/data/partitionedData")
		/*println("-----task----")
		val structschema1 = StructType(Array(
		    StructField("first_name",StringType,true),
		    StructField("last_name",StringType,true),
		    StructField("company_name",StringType,true),
		    StructField("address",StringType,true),
		    StructField("city",StringType,true),
		    StructField("country",StringType,true),
		    StructField("state",StringType,true),
		    StructField("zip",StringType,true),
		    StructField("age",StringType,true)))
		val df =spark.read.schema(structschema1).format("csv").load("file:///C:/data/usdata.csv")
		df.show()*/
		
		    
		/*val rowdf=spark.createDataFrame(rowrdd,structschema)
		rowdf.show()
		rowdf.createOrReplaceTempView("txndf")
		val finaldata=spark.sql("select * from txndf where spendby='cash'")
		finaldata.show() */
}
}