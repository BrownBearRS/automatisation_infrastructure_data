package esgi.circulation
import org.apache.spark.sql.SparkSession

import java.util.logging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level

object Clean {
  def main(args: Array[String]): Unit = {

   // Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession
      .builder()
      .appName("Data cleaner : Conversion csv to Parquet")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate();

    val inputFile = args(0)
    val outputFile = args(1)

    // Lecture du fichier d'input

    val df = spark.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .option("path", "C:\\Users\\steph\\Desktop\\Cours\\5IABD\\Semestre 1\\Automatisation et Infrastructure\\esgi-tp-data-main\\src\\data\\comptages-routiers-permanents_1.csv")
      .load();

    df.printSchema()


    val filterData = df.select("*");
    println(filterData);

    // TODO : ajouter 3 colonnes à votre dataframe pour l'année, le mois et le jour

    // def add_column( ): Unit ={


    // TODO : écrire le fichier en parquet et partitionné par année / mois / jour
    filterData.write.option("compression", "none")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("C:\\Users\\steph\\Desktop\\Cours\\5IABD\\Semestre 1\\Automatisation et Infrastructure\\esgi-tp-data-main\\src\\data\\Clean_Data")

    println("Clean_Data");

    // Montrer les données au format parquet
    spark.read.parquet("C:\\Users\\steph\\Desktop\\Cours\\5IABD\\Semestre 1\\Automatisation et Infrastructure\\esgi-tp-data-main\\src\\data\\Clean_Data").show();
    spark.close()

  }
}
