package nyu.bdad.tars

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object NB{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Final Project")
      .getOrCreate()

    val basePath = args(0)
    val outPath = args(1)
    val missing = args(2).toFloat
    val lambda = args(3).toFloat
    val exp = args(4)

    nb(spark, basePath, outPath, missing, lambda, exp)

    val misStr = missing.toString
    val lamStr = lambda.toString
    val outputdir = s"${outPath}/${exp}_mis_${misStr}_lam_${lamStr}_test"
    evaluate(spark, outputdir, outPath)

  }

  def evaluate(spark: SparkSession, path: String, outDir: String): Unit = {

    import spark.implicits._

    def getDf(filePath: String) = {

      val schema = new StructType().add(
        "wh_user_id",IntegerType,false).add(
          "wh_video_id",IntegerType,false).add(
            "label",IntegerType,false).add(
            "p_watch",FloatType,false).add(
              "p_not",FloatType,false).add(
                "count",IntegerType,false).add(
                  "frac",FloatType,false).add(
                    "watched",FloatType,false).add(
                      "not",FloatType,false)

      val df = spark.read.option("header",true).schema(schema).csv(filePath).withColumn(
          "PW", $"p_watch"+$"watched").withColumn(
            "PN", $"p_not"+$"not")

      df
    }

    def tryThreshold(df: DataFrame, threshold: Float) = {
      val df_pred = df.withColumn("pred", $"PW" + threshold > $"PN").withColumn(
        "pred",$"pred".cast("Int")).withColumn("correct", $"pred"===$"label")

      df_pred
    }

    def getAcc(df: DataFrame) = {
      val a = df.groupBy("correct").count().collect()
      val perc = (a(0).getAs[Long](1)).toFloat/(a(0).getAs[Long](1)+a(1).getAs[Long](1))
      (a(0).getAs[Boolean](0), a(0).getAs[Float](1), a(1).getAs[Boolean](0), a(1).getAs[Float](1), perc)
    }

    def getPrecision(df: DataFrame) = {
      val a = df.filter($"label"===1).groupBy("correct").count().collect()
      val perc = (a(0).getAs[Long](1)).toFloat/(a(0).getAs[Long](1)+a(1).getAs[Long](1))
      (a(0).getAs[Boolean](0), a(0).getAs[Float](1), a(1).getAs[Boolean](0), a(1).getAs[Float](1), perc)
    }

    def getRecall(df: DataFrame) = {
      val a = df.filter($"pred"===1).groupBy("correct").count().collect()
      val perc = (a(0).getAs[Long](1)).toFloat/(a(0).getAs[Long](1)+a(1).getAs[Long](1))
      (a(0).getAs[Boolean](0), a(0).getAs[Float](1), a(1).getAs[Boolean](0), a(1).getAs[Float](1), perc)
    }

    def getTrending(df0: DataFrame): Float = {
      val w = Window.orderBy(desc("count"))

      val realT = df0.filter($"label"===1).groupBy(
        "wh_video_id").count().withColumn("rank", rank.over(w)).where($"rank" <= 100)
      val predT = df0.filter($"pred"===1).groupBy(
        "wh_video_id").count().withColumn("rank", rank.over(w)).where($"rank" <= 100)

      val merged = predT.join(realT, "wh_video_id")

      merged.count.toFloat/100
    }

    val df = getDf(path)
    val df1 = tryThreshold(df, 0)

    val acc = getAcc(df1)
    val pre = getPrecision(df1)
    val rec = getRecall(df1)
    val tre = getTrending(df1)

    val result = s"Predicting watch history. Accuracy is ${acc._5}, precision is ${pre._5}, recall is ${rec._5}. Predicting trending. Accuracy is ${tre}"

    val sc = spark.sparkContext
    sc.parallelize(Seq(result)).coalesce(1).toDF().as[String].write.mode(SaveMode.Overwrite).text(s"${outDir}/result.txt")
  }

  def nb(spark: SparkSession, basePath: String, outPath: String, missing: Float, lambda: Float, exp: String): Unit = {

    import spark.implicits._

    val expStr = exp.toString
    val misStr = missing.toString
    val lamStr = lambda.toString

    val whPath = basePath + "wh/"
    val vidPath = basePath + "video/"
    val userPath = basePath + "user/"

    val schema = new StructType().add(
      "wh_user_id",IntegerType,false).add(
        "wh_video_id",IntegerType,false).add(
          "view_date",StringType,false).add(
            "view_duration",FloatType,false)

    val vid_schema = new StructType().add(
      "video_id",IntegerType,false).add(
      "title",StringType,false).add(
      "country",StringType,false).add(
      "lat",FloatType,false).add(
      "long",FloatType,false).add(
      "duration_bucket",StringType,false).add(
      "duration",IntegerType,false).add(
      "category",StringType,false).add(
      "max_available_resolution",StringType,false).add(
      "posted_on",StringType,false).add(
      "url",StringType,false)

    val user_schema = new StructType().add(
      "index",IntegerType,false).add(
      "join_date", StringType, false).add(
      "first",StringType,false).add(
      "last",StringType,false).add(
      "age",IntegerType,false).add(
      "gender",StringType,false).add(
      "occupation",StringType,false).add(
      "location",StringType,false).add(
      "lat",FloatType,false).add(
      "long",FloatType,false).add(
      "user_id",IntegerType,false).add(
      "location_bias",FloatType,false).add(
      "toleration_level",FloatType,false).add(
      "resolution_preference",StringType,false).add(
      "interest",StringType,false)

    def convertDateTime = udf {s: String => s.substring(0, 10) + " " + s.substring(11, 19)}

    val users = spark.read.option("header",true).schema(user_schema).csv(
      userPath).select("user_id").withColumnRenamed("user_id", "wh_user_id").coalesce(1)

    val df = spark.read.option("header",true).schema(schema).csv(whPath).withColumnRenamed(
      "wh_video_id", "vid_id").select("wh_user_id", "vid_id").coalesce(10)
    df.persist()

    val videos = spark.read.option("header",true).schema(vid_schema).csv(
      vidPath).withColumnRenamed("video_id", "vid_id"
        ).select("vid_id","country","duration_bucket","category","max_available_resolution","posted_on").withColumn(
          "posted_on", convertDateTime($"posted_on")).withColumn(
          "epoch", unix_timestamp($"posted_on", "yyyy-MM-dd HH:mm:ss")
        ).coalesce(4)
    videos.persist()

    def trainTestSplit(wh: DataFrame, vid: DataFrame, users: DataFrame) = {
      val train_vid = vid.filter($"epoch" <= 1600000000).coalesce(10) // .sample(true, 0.05, 42)
      val test_vid = vid.filter($"epoch" > 1600000000).coalesce(10)
      val unique_watched = wh.groupBy("vid_id", "wh_user_id").count().select(
        "vid_id", "wh_user_id").withColumn("label", lit(1)).sample(false, missing, 42) // simulate missing training data
      unique_watched.persist()

      val train_not = unique_watched.join(users.crossJoin(train_vid),
        Seq("vid_id","wh_user_id"), "right").na.fill(0).filter($"label"<1)
      val train_watched = unique_watched.join(vid, "vid_id")
      val train = train_not.union(train_watched)

      val unique_watched_full = wh.groupBy("vid_id", "wh_user_id").count().select(
        "vid_id", "wh_user_id").withColumn("label", lit(1))
      val test = users.crossJoin(test_vid).join(unique_watched_full,
        Seq("vid_id","wh_user_id"), "left").na.fill(0)

      (train, test)
    }

    val (traindf, testdf) = trainTestSplit(df, videos, users)
    traindf.persist()
    testdf.persist()

    // features crossJoin user
    val features = Array("country", "duration_bucket", "category", "max_available_resolution")
    val feature_df = features.map(
      feature => videos.groupBy(feature).count().select(feature).crossJoin(users))
    feature_df.map(df => df.persist()) // use all videos when generating feature

    // get naive bayes features
    // percentage watched
    val n = videos.count
    val fraction = ((a: Float) => a / n)
    val fracUDF = udf(fraction)
    val p_watched = df.groupBy("wh_user_id").count().withColumn(
      "percent", fracUDF($"count")).withColumn(
      "frac_watched", log($"percent")).withColumn(
      "frac_not", log(lit(1) - $"percent")
    )

    p_watched.persist()

    // conditional probability of features on watched (pw_), and not watched (pnw_)
    def train(wat: DataFrame, ind: Int) = {
      val total = wat.groupBy("wh_user_id").count().withColumnRenamed("count", "total")
      val byFeature = wat.groupBy("wh_user_id", features(ind)).count()
      val pw_feature = feature_df(ind).join(
        byFeature, Seq("wh_user_id", features(ind)),"left"
      ).join(total, "wh_user_id").na.fill(0, Seq("count")).withColumn("count", $"count" + lit(lambda))

      pw_feature.withColumn(
        "p", log(pw_feature("count")/pw_feature("total"))
      )
    }

    // get conditional probability
    val watched = traindf.filter($"label">0)
    val notwatched = traindf.filter($"label"<1)

    val pw = Seq(0, 1, 2, 3).map(ind => train(watched, ind))
    val pnw = Seq(0, 1, 2, 3).map(ind => train(notwatched, ind))
    pw.map(df => df.persist())
    pnw.map(df => df.persist())

    // train accuracy
    def predictP(user_vid: DataFrame, cond_p: Seq[DataFrame], pname: String) = {
      var result = user_vid
      for( ind <- 0 to 3){
        result = result.join(
          cond_p(ind).select("wh_user_id", features(ind), "p").withColumnRenamed("p", s"p_$ind"),
          Seq("wh_user_id", features(ind)), "left").na.fill(0)
      }

      result.withColumn(pname, $"p_0" + $"p_1" + $"p_2" + $"p_3") // adding log of probability
    }

    def getAcc(wat: DataFrame, traintest: String) = {
      val predict_watched = predictP(wat, pw, "p_watch")
      val predict_not = predictP(wat, pnw, "p_not")

      val predict_temp = predict_watched.select("label", "wh_user_id", "vid_id", "p_watch").join(
        predict_not.select("wh_user_id", "vid_id", "p_not"), Seq("wh_user_id", "vid_id")
      ).join(p_watched, "wh_user_id")
      predict_temp.persist()

      predict_temp.write.mode(SaveMode.Overwrite).csv(
        s"${outPath}/${expStr}_mis_${misStr}_lam_${lamStr}_${traintest}"
      )

      1
    }

    getAcc(testdf, "test")
  }

}

