import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


object cdrusesparksession {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("CDR USING SPARKSESSION")
        .master("local")
        .getOrCreate()
    
      val fileschema = StructType (Array(
                      StructField("call_locn", IntegerType, true),
                      StructField("call_duration", IntegerType, true),
                      StructField("phone_no", StringType, true),
                      StructField("error_code", StringType, true)))
                    
     val filepath = "/home/nakul/CDR.csv"
     val filedf = spark.read.format("csv")
                 .option("header","false")
                 .schema(fileschema)
                 .load(filepath)
                 .cache()
   
   filedf.createOrReplaceTempView("roaming_data")
   
   val result = spark.sql("select phone_no , count(*) as call_drops from roaming_data group by phone_no order by call_drops desc limit 10")
  
   result.rdd.saveAsTextFile("/home/nakul/CDR_Results")
   result.rdd.foreach(println)
   
                 
                 
    
    
  }
  
}