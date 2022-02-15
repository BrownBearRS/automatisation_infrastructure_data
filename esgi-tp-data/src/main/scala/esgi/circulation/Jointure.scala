package esgi.circulation
from pyspark.sql import SparkSession



object Jointure {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)


    val df_inputFile = inputFile.spark.read.format("parquet")
    val df_joinFile = joinFile.spark.read.format("parquet")

    
    val outputFile_df = df_inputFile.join(df_joinFile, df_inputFile("iu_ac") === df_joinFile("iu_ac"))

    outputFile_df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(outputFile)
  }
}