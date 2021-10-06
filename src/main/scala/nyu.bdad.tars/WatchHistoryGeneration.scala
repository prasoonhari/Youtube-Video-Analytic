
package nyu.bdad.tars

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, floor, lit, randn, row_number, to_date, to_timestamp}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.when


object WatchHistoryGeneration {

  val relativeUserPath = "/user/"
  val relativeVideoPath = "/video/"
  val relativeWHPath = "/wh/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TARS")
      .config("spark.app.name", "TARS")
      .config("spark.master", "yarn")
      .config("spark.sql.autoBroadcastJoinThreshold", 0)
      .getOrCreate()


    val categoryData = args(0)
    println("fetching category data from path = " + categoryData)

    val userData = args(1)
    println("fetching user data from path = " + userData)

    val videoData = args(2)
    println("fetching video data from path = " + videoData)

    val userMetaData = args(3)
    println("fetching user meta data from path = " + userMetaData)

    val experimentPath = args(4)
    println("experiment path will be  = " + experimentPath)

    val userSampleFraction = args(5).toFloat
    println("user sample fraction  = " + userSampleFraction)

    val videoSampleFraction = args(6).toFloat
    println("video sample fraction  = " + videoSampleFraction)

    val userOutPath = experimentPath + relativeUserPath
    println("userOutPath will be  = " + userOutPath)

    val videoOutPath = experimentPath + relativeVideoPath
    println("videoOutPath will be  = " + videoOutPath)

    val whOutPath = experimentPath + relativeWHPath
    println("whOutPath will be  = " + relativeWHPath)

    val categoryDF = getCategoryDF(spark, categoryData)
    val baseUserDF = augmentUserDF(spark, userData, userMetaData, userSampleFraction, userOutPath)
    val baseVideoDF = augmentVideoDF(spark, categoryDF, videoData, videoSampleFraction,  videoOutPath)

    val userWorkingSetDF = baseUserDF.select("user_id", "age", "interest", "location", "views_from_low", "views_from_high", "views_from_mid")
    val videoWorkingSetDF = baseVideoDF.select("video_id", "target_age", "sd", "category", "country")

    val weightedVideosDF = computeWeightedVideosForEachUser(spark, userWorkingSetDF, videoWorkingSetDF)
    val userToVideoTrimmedDF = weightedVideosDF.select("user_id", "views_from_bucket", "weight_bucket", "video_id")

    val w= Window.partitionBy("user_id","weight_bucket", "views_from_bucket").orderBy("weight_bucket")
    val partitionedOnUserAndWeight : DataFrame = userToVideoTrimmedDF.withColumn("rank", row_number.over(w)).filter(col("rank") < col("views_from_bucket")).drop(col("rank")).persist()

    var daysDeltaAddedToDF =  partitionedOnUserAndWeight.withColumn("normal_dist", randn(60L))
    daysDeltaAddedToDF = daysDeltaAddedToDF.withColumn("days_delta", floor(col("normal_dist").multiply(10) + lit(100)))
    daysDeltaAddedToDF = daysDeltaAddedToDF.withColumn("base_date", to_date(lit("20-01-2021"), "dd-MM-yyyy"))
    daysDeltaAddedToDF = daysDeltaAddedToDF.withColumn("view_date", expr("date_add(base_date, days_delta)"))

    var watchHistoryIntermediateDF = daysDeltaAddedToDF.select("user_id","video_id", "view_date")
    watchHistoryIntermediateDF = watchHistoryIntermediateDF.withColumnRenamed("user_id", "wh_user_id")
    watchHistoryIntermediateDF = watchHistoryIntermediateDF.withColumnRenamed("video_id", "wh_video_id")

    var watchHistoryWithVideoDuration = watchHistoryIntermediateDF.join(baseVideoDF, col("wh_video_id").equalTo(col("video_id")), "left")
    watchHistoryWithVideoDuration = watchHistoryWithVideoDuration.select("wh_user_id", "wh_video_id", "view_date", "duration", "duration_bucket")

    var watchHistoryWithUserPreferenceForDuration = watchHistoryWithVideoDuration.join(baseUserDF, col("wh_user_id").equalTo(col("user_id")), "left")
    watchHistoryWithUserPreferenceForDuration = watchHistoryWithUserPreferenceForDuration.select("wh_user_id", "wh_video_id", "view_date", "duration", "duration_bucket", "toleration_level")

    watchHistoryWithUserPreferenceForDuration = watchHistoryWithUserPreferenceForDuration.withColumn("toleration_bucket", when(col("toleration_level").lt(lit(0.5)), lit("low")).otherwise(lit("high")))
    watchHistoryWithUserPreferenceForDuration = watchHistoryWithUserPreferenceForDuration.withColumn("view_duration", when(col("duration_bucket").isin("Tiring", "Lengthy"), when(col("toleration_bucket").equalTo("high"), col("toleration_level").multiply(col("duration"))).otherwise(col("toleration_level").multiply(col("duration")))).otherwise(lit(0.8).multiply(col("duration"))))

    val watchHistoryDF = watchHistoryWithUserPreferenceForDuration.select("wh_user_id", "wh_video_id", "view_date", "view_duration")
    val watchHistoryCachedDF = watchHistoryDF.cache()
    watchHistoryCachedDF.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(whOutPath)

    spark.stop()
  }

  private def getCategoryDF(spark: SparkSession, categoryData : String): DataFrame = {
    var categoryDF = spark.read.format("csv").option("header", "true").load(categoryData)
    categoryDF = categoryDF.withColumnRenamed("category", "seed_category")

    categoryDF
  }

  private def augmentUserDF(spark: SparkSession, userData : String, userMetaData : String, userSampleFraction : Float, userOutPath : String): DataFrame = {
    val userDF = spark.read.format("csv").option("header", "true").load(userData)
    val userSampledDF = userDF.sample(false, userSampleFraction, 1L)
    userSampledDF.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(userOutPath)

    val sampledUserReadDF = spark.read.format("csv").option("header", "true").load(userOutPath)

    var userMetaDF = spark.read.format("csv").option("header", "true").load(userMetaData)
    userMetaDF = userMetaDF.withColumnRenamed("user_id","user_id_from_meta").drop("_c0")

    val userJoinedDF =  sampledUserReadDF.join(userMetaDF, col("user_id").equalTo(col("user_id_from_meta")), "left")
    val baseUserDF = userJoinedDF.withColumn("mod_join_date", to_timestamp(col("join_date"))).drop("_c0")

    baseUserDF
  }

  private def augmentVideoDF(spark: SparkSession, categoryDF : DataFrame, videoData : String, videoSampleFraction : Float, videoOutPath : String): DataFrame = {
    val videoDF = spark.read.format("csv").option("header", "true").load(videoData)
    val sampledVideoDF = videoDF.sample(false, videoSampleFraction, 1L)
    sampledVideoDF.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(videoOutPath)

    val sampledVideoReadDF = spark.read.format("csv").option("header", "true").load(videoOutPath)
    val videoJoinedDF = sampledVideoReadDF.join(categoryDF, col("category").equalTo(col("seed_category")), "left").drop("seed_category")

    videoJoinedDF
  }

  private def computeWeightedVideosForEachUser(spark: SparkSession, userWorkingSetDF : DataFrame, videoWorkingSetDF : DataFrame): DataFrame = {
    videoWorkingSetDF.repartition(500).cache()
    videoWorkingSetDF.show(10)
    val userJoinedWithVideos = userWorkingSetDF.crossJoin(videoWorkingSetDF)
    val userJoinedWithVideosWithWeights = userJoinedWithVideos.withColumn("weight", lit(0))

    val userToVideoInterestWeight = userJoinedWithVideosWithWeights.withColumn("weight", when(col("interest").equalTo(col("category")), col("weight") + 0.5).otherwise(col("weight")))
    val userToVideoLocationWeight = userToVideoInterestWeight.withColumn("weight", when(col("location").equalTo(col("country")), col("weight") + 0.4).otherwise(col("weight")))
    val userToVideoAgeWeight = userToVideoLocationWeight.withColumn("weight", when(col("age").gt(col("target_age") - col("sd")).and(col("age").lt(col("target_age") + col("sd"))), col("weight") + 0.1).otherwise(col("weight")))

    val userToVideoWeightBucket = userToVideoAgeWeight.withColumn("weight_bucket", when(col("weight").lt(lit(0.2)), lit("low")).otherwise(when(col("weight").lt(lit(0.5)).and(col("weight").gt(lit(0.2))), lit("mid")).otherwise("high")))

    val userToVideoWeightBucketWithNormalizedCol = userToVideoWeightBucket.withColumn("views_from_bucket", when(col("weight_bucket").equalTo(lit("low")), col("views_from_low")).otherwise(when(col("weight_bucket").equalTo(lit("mid")), col("views_from_mid")).otherwise(col("views_from_high"))))

    userToVideoWeightBucketWithNormalizedCol
  }
}