
package nyu.bdad.tars

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lower, to_timestamp}


object VideoDataCleaning {

  val relativeVideoPath = "/video/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TARS")
      .config("spark.app.name", "TARS")
      .config("spark.master", "yarn")
      .getOrCreate()

    val videoData = args(0)
    println("fetching video data from path = " + videoData)

    val cleanData = args(1)
    println("cleanData path will be  = " + cleanData)

    val videoOutPath = cleanData + relativeVideoPath
    println("videoOutPath will be  = " + videoOutPath)

    val baseVideoDF : DataFrame = spark.read.format("csv").option("header", "true").load(videoData)
    baseVideoDF.show(10, false)

    //Drop extra and irrelevant column
    var baseVideoCleanedDF = baseVideoDF.drop("_c0")

    //Removing null values
    baseVideoCleanedDF = baseVideoCleanedDF.na.drop()

    //Removing case ambiguities
    baseVideoCleanedDF = baseVideoCleanedDF.withColumn("duration_bucket", lower(col("duration_bucket")))

    //Removing noisy urls
    val baseVideoCleanedDFUrl = baseVideoCleanedDF.filter(col("url").contains("http://youtube.com/videoId"))

    //Rectifying the date formats
    val dfWithCleanedDates = baseVideoCleanedDFUrl.withColumn("posted_on", to_timestamp(col("posted_on")))

    dfWithCleanedDates.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(videoOutPath)

  }
}
