import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, FloatType}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object App {
      
      def main(args: Array[String]):Unit={
        println("Hello, world!")

         val spark = SparkSession
         .builder()
         .appName("job-tp")
         .master("local[*]")
         .getOrCreate()

         val code_p_df = spark
         .read.option("header",true)
         .option("delimiter", ";")
         .csv("/Users/Leila/tp-spark/code-insee-postaux-geoflar.csv")
         code_p_df.show()
         code_p_df.printSchema()

         val info_com_df = spark
         .read
         .option("header",true)
         .option("delimiter", ";")
         .csv("/Users/Leila/tp-spark/Communes.csv")

         info_com_df.show()
         info_com_df.printSchema()

         val s_df = spark
         .read
         .option("header",true)
         .option("delimiter", ";")
         .csv("/Users/Leila/tp-spark/synop.2020120512.txt")

         s_df.show()
         s_df.printSchema()

         val syn_df = spark
         .read.option("header",true)
         .option("delimiter", ";")
         .csv("/Users/Leila/tp-spark/postesSynop.txt")

         syn_df.show()
         syn_df.printSchema()

         

         //csv_df_v2.write.parquet("csv_df_v2_parquet_file.parquet")


     }
}