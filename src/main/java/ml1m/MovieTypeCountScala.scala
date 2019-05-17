package ml1m

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * ClassName SparkSQLDemo1
  * Author    MxRanger
  * Date      2019/4/5
  * Time      9:40
  *
  * scala实现sparksql  电影类别用户分布
  *
  **/
object MovieTypeCountScala {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val spark = SparkSession.builder().appName("sparkSQL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()

    val rdd1 = spark.sparkContext.textFile("file:///F:/sparktest/recommend/movie_info.csv")



    //标一成对
    val rdd2 = rdd1.map(line=>{
      //val types: ArrayBuffer[String] = null
      val types = line.split(";")(2)
      (types)
    })

    val rdd3 = rdd2.flatMap(_.split("\\|"))

    val rdd4 = rdd3.map((_,1))

    import spark.implicits._
    val df1 = rdd4.reduceByKey(_+_).toDF("type","num")
    //将数据框做成临时视图
    df1.createOrReplaceTempView("_typenumInfo")
    spark.sql("select type , num from _typenumInfo order by num desc").show(10,false)

    spark.sql("select type , num from _typenumInfo order by num desc").write.json("f:/sparktest/recommend/ml1mresult/type_num")
  }
}
