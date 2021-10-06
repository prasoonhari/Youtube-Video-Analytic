package nyu.bdad.tars
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object UserDataCleaning {

  val relativeUserPath = "/user/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TARS")
      .config("spark.app.name", "TARS")
      .config("spark.master", "yarn")
      .getOrCreate()

    val userData = args(0)
    println("fetching user data from path = " + userData)

    val cleanData = args(1)
    println("cleanData path will be  = " + cleanData)

    val userOutPath = cleanData + relativeUserPath
    println("userOutPath will be  = " + userOutPath)

    val location = List("Canada","US","UK")
    val valid_gender_string = List("M", "F", "O", "MALE","Female","f","FEMALE")
    
    val user_data = spark.sparkContext.textFile(userData)

    val header_RDD = user_data.first()

    val user_data_noheader = user_data.filter(line => !line.contains("toleration_level"))

    val user_cleanEmptyData = user_data_noheader.filter(line => !line.contains(",,"))

    val user_cleanUrl = user_cleanEmptyData.filter(line => line.split(',')(15).contains("http://youtube.com/user="))
    val user_cleanBais = user_cleanUrl.filter(line => (0<=line.split(',')(12).toDouble&&line.split(',')(12).toDouble<=1) && (0<=line.split(',')(11).toDouble&&line.split(',')(11).toDouble<=1))
    
    val user_cleanLocation = user_cleanBais.filter(line => location.contains(line.split(',')(7)))
    // user_cleanLocation.take(100).foreach(println)
    val user_cleanAge = user_cleanLocation.filter(line => (5<=line.split(',')(4).toInt&&line.split(',')(4).toDouble<=80) )


    val user_cleanDate = user_cleanAge.filter(line => (line.split(',')(1).split('-')(2).contains(':') &&line.split(',')(1).split('-')(0).toInt<2020 &&  line.split(',')(1).split('-')(1).toInt<13 && line.split(',')(1).split('-')(1).toInt>0))

    
    val user_cleanGender = user_cleanDate.filter(line => valid_gender_string.contains(line.split(',')(5)))

    val user_schema = StructType(Array(
    StructField("index",StringType,true),
    StructField("join_date",StringType,true),
    StructField("first",StringType,true),
    StructField("last", StringType, true),
    StructField("age", StringType, true),
    StructField("gender", StringType, true),
    StructField("occupation",StringType,true),
    StructField("location",StringType,true),
    StructField("lat",StringType,true),
    StructField("long", StringType, true),
    StructField("user_id", StringType, true),
    StructField("location_bias", StringType, true),
    StructField("toleration_level",StringType,true),
    StructField("resolution_preference", StringType, true),
    StructField("interest", StringType, true),
    StructField("url", StringType, true)
  ))

    val clean_result_rdd = user_cleanGender.map(x => x.split(","))
    val rdd_seq = clean_result_rdd.map(a => Row.fromSeq(a))
    val clean_user_data = spark.createDataFrame(rdd_seq, org.apache.spark.sql.types.StructType(user_schema))

    clean_user_data.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(userOutPath)

  }
}
