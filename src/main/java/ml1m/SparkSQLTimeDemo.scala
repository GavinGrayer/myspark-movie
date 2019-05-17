package ml1m

import java.text.{DateFormat, SimpleDateFormat}
import java.time.LocalDate
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * ClassName SparkSQLDemo1
  * Author    MxRanger
  * Date      2019/4/5
  * Time      9:40
  *
  * scala实现sparksql  时间段分布分析
  *
  **/

object SparkSQLTimeDemo {

  /*
  *  [1]电影评论数排名
  * */
  def timeRange(spark:SparkSession): DataFrame ={

    init(spark)

    val comments_sql = "select  timestamp , count(userId) as people_num " +
      "from _movie_time_Info" +
      " group by timestamp" +
      " order by count(userId) desc"
    //spark.sql(comments_sql).write.json("F:\\sparktest\\movie\\comments")
    spark.sql(comments_sql)
  }

  def init(spark:SparkSession): DataFrame ={

    val rdd1 = spark.sparkContext.textFile("file///f:/sparktest/recommend/movie_time_info.csv")
    //val rdd2 = rdd1.flatMap(_.split("\t"))
    val rdd2 = rdd1.map(line=>{
      val arr = line.split(";")
      var flag = 0
        (
          arr(0),
          arr(1),
          arr(2),
          arr(3),
          arr(4),
          tranTimeToString(arr(5).toLong)
          //arr(5)
        )
    })

    //导入sparksession的隐式转换
    import spark.implicits._
    //将rdd转换成数据框
    val df = rdd2.toDF("movieId","title","genres","userId","rating","timestamp")

    //将数据框做成临时视图
    df.createOrReplaceTempView("_movie_time_Info")
    df
  }

  def tranTimeToString(time:Long) :String={
    //val newtime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time*1000)
    val newtime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time*1000))
    val newtime2 :Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(newtime)

    var hour = newtime2.getHours
    var timetype:String=null

    if(hour>0 && hour <=6){
      timetype = "凌晨 0 - 6"
    }else if(hour>6 && hour <=12){
      timetype = "上午 6 - 12"
    }else if(hour>12 && hour <=18){
      timetype = "下午 12 - 18"
    }else{
      timetype = "晚上 18 - 24"
    }
    timetype
  }

  def main(args: Array[String]): Unit = {
    var start_time =new Date().getTime

    val spark = SparkSession.builder().appName("sparkSQL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()

    /*
    *  [1]电影评论数排名
    */
    timeRange(spark).show(50,false)
    timeRange(spark).write.json("f:/sparktest/recommend/ml1mresult/timerange")

    var end_time =new Date().getTime
    println("总时长:"+(end_time - start_time))
}

  def main2(args: Array[String]): Unit = {
    val time:Long= 1513839667//秒
    val newtime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time*1000)
    println(newtime)
    println(tranTimeToString(time))
  }

}
