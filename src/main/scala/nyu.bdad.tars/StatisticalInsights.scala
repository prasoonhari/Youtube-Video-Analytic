package nyu.bdad.tars

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, desc, from_unixtime,unix_timestamp}

object StatisticalInsights {

  // java -jar statistical.jar "category_path" "user_path" "video_path" "wh_path" "clean_path_for_output"
  val relativeStatPath = "/statistical/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TARS - Statistical Analysis")
      .config("spark.app.name", "TARS")
      .config("spark.master", "yarn")
      .getOrCreate()

    val categoryData = args(0)
    println("fetching category data from path = " + categoryData)

    val userData = args(1)
    println("fetching user data from path = " + userData)

    val videoData = args(2)
    println("fetching video data from path = " + videoData)

    val watchHistoryData = args(3)
    println("fetching watch history data from path = " + watchHistoryData)

    val cleanPath = args(4)
    println("cleanPath path will be  = " + cleanPath)

    val outBasePath = cleanPath + relativeStatPath
    println("statOutPath will be  = " + outBasePath)

    val categoryDF = getCategoryDF(spark,categoryData)
    val userDF = getUserDF(spark, userData)
    val videoDF = getVideoDF(spark, videoData)
    val whDF = getWatchHistoryDF(spark, watchHistoryData)

    var whJoinedWithUserDF = whDF.join(userDF, whDF.col("wh_user_id").equalTo(userDF.col("user_id")), "left")
    whJoinedWithUserDF = whJoinedWithUserDF.drop("user_id")

    var whJoinedWithVideoDF = whJoinedWithUserDF.join(videoDF, whJoinedWithUserDF.col("wh_video_id").equalTo(videoDF.col("video_id")), "left")
    whJoinedWithVideoDF = whJoinedWithVideoDF.drop("video_id")

    var whJoinedWithCategoryDF = whJoinedWithVideoDF.join(categoryDF, whJoinedWithVideoDF.col("category").equalTo(categoryDF.col("seed_category")), "left")
    whJoinedWithCategoryDF = whJoinedWithCategoryDF.drop("seed_category")

    val watchHistoryDF = whJoinedWithCategoryDF.limit(10000).cache()
    watchHistoryDF.show(10, false)

    // Query 1 : Users with maximum watch count (descending order)
    val selectedColumnsForUser = watchHistoryDF.select(col("wh_user_id").as("user_id"), col("first"), col("last"), col("location")).dropDuplicates()
    val userWithMostWatchCount = watchHistoryDF.groupBy("wh_user_id").agg(count("wh_video_id").as("number_of_videos_watched")).orderBy(desc("number_of_videos_watched")).limit(10)
    val userWithMostWatchCountWithDetails = selectedColumnsForUser.join(userWithMostWatchCount,col("wh_user_id").equalTo(col("user_id"))).drop("wh_user_id").orderBy(desc("number_of_videos_watched")).limit(10)
    userWithMostWatchCountWithDetails.show()
    userWithMostWatchCountWithDetails.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_1")

    // Query 2 : Countries sorted by the number of users it possess (descending order)
    val countrySortedWithUserCount = watchHistoryDF.groupBy("location").agg(count("wh_user_id").as("number_of_users")).orderBy(desc("number_of_users"))
    countrySortedWithUserCount.show()
    countrySortedWithUserCount.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_2")

    // Query 3 : Top 5 most popular categories in US (descending order)
    val popularCategoriesInUSA = watchHistoryDF.select("category","location","wh_user_id").filter("location == 'US'").groupBy("category").agg(count("wh_user_id").as("view_count")).orderBy(desc("view_count")).limit(5)
    popularCategoriesInUSA.show()
    popularCategoriesInUSA.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_3")

    // Query 4 : Top 4 preferred video duration by users (descending order)
    val preferredVideoDuration = watchHistoryDF.groupBy(col("duration_bucket").as("video_duration")).agg(count("wh_user_id").as("user_count")).orderBy(desc("user_count")).limit(4)
    preferredVideoDuration.show()
    preferredVideoDuration.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_4")

    // Query 5 : Top 10  most watched videos of 2021 (descending order)
    val selectedVideoFields = watchHistoryDF.select(col("wh_video_id").as("video_id"), col("title")).dropDuplicates()
    val mostWatchedVideo = watchHistoryDF.select("wh_video_id","wh_user_id", "view_date").filter(watchHistoryDF("view_date").geq("2021-01-01")).groupBy("wh_video_id").agg(count("wh_user_id").as("view_count")).orderBy(desc("view_count"))
    val mostWatchedVideoWithDetails = selectedVideoFields.join(mostWatchedVideo,col("wh_video_id").equalTo(col("video_id"))).drop("wh_video_id").orderBy(desc("view_count")).limit(10)
    mostWatchedVideoWithDetails.show()
    mostWatchedVideoWithDetails.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_5")

    // Query 6 : Top 3 most preferred resolutions by users (descending order)
    val preferredResolution = watchHistoryDF.groupBy("resolution_preference").agg(count("wh_user_id").as("user_count")).orderBy(desc("user_count")).limit(3)
    preferredResolution.show()
    preferredResolution.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_6")

    // Query 7 : 5 Least popular categories amongst females (ascending order)
    val leastPopulatCategoriesAmongstFemales = watchHistoryDF.select("category","wh_user_id","gender").filter("gender == 'F'").groupBy("category").agg(count("wh_user_id").as("view_count")).orderBy(("view_count")).limit(5)
    leastPopulatCategoriesAmongstFemales.show()
    leastPopulatCategoriesAmongstFemales.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_7")

    // Query 8 : View counts in different months of the year 2021 (descending order)
    val ColumnMonth = watchHistoryDF.withColumn("month", from_unixtime(unix_timestamp(col("view_date"), "yyyy-MM-dd"), "MMMMM"))
    //var selectedFields = ColumnMonth.select("view_date","month")
    val monthsSortedOnViewCount = ColumnMonth.filter(watchHistoryDF("view_date").geq("2021-01-01") and watchHistoryDF("view_date").leq("2021-12-31")).groupBy("month").agg(count("wh_user_id").as("view_count")).orderBy(desc("view_count"))
    monthsSortedOnViewCount.show()
    monthsSortedOnViewCount.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_8")

    // Query 9 : Category sorted with user count  (For pi distribution)
    val categoriesSortedWithUserCount = watchHistoryDF.groupBy("category").agg(count("wh_user_id").as("number_of_users")).orderBy(desc("number_of_users"))
    categoriesSortedWithUserCount.show()
    categoriesSortedWithUserCount.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_9")

    // Query 10 : Occupations sorted with view count (descending order)
    val occupationWithMaximumViewCount = watchHistoryDF.groupBy("occupation").agg(count("wh_user_id").as("view_count")).orderBy(desc("view_count"))
    occupationWithMaximumViewCount.show()
    occupationWithMaximumViewCount.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(outBasePath + "query_10")

  }

  private def getCategoryDF(spark: SparkSession, categoryData : String): DataFrame = {
    var categoryDF = spark.read.format("csv").option("header", "true").load(categoryData)
    categoryDF = categoryDF.withColumnRenamed("category", "seed_category")
    categoryDF
  }

  private def getUserDF(spark: SparkSession, userData : String): DataFrame = {
    val userDF = spark.read.format("csv").option("header", "true").load(userData)
    userDF
  }

  private def getVideoDF(spark: SparkSession, videoData : String): DataFrame = {
    var videoDF = spark.read.format("csv").option("header", "true").load(videoData)
    videoDF
  }

  private def getWatchHistoryDF(spark: SparkSession, watchHistoryData : String): DataFrame = {
    val whDF = spark.read.format("csv").option("header", "true").load(watchHistoryData)
    whDF
  }

}
