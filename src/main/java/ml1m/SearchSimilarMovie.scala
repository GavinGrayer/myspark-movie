package ml1m

import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  *
  * ClassName SearchSimilarMovie
  * Author    MxRanger
  * Date      2019/4/11
  * Time      20:31
  *
  **/

object SearchSimilarMovie {

  def isExist(types:ArrayBuffer[String],str:String): Boolean ={
    types.foreach(e=>{
      if(e.equals(str)){
        return true
      }
    })
    false
  }

  def init(spark:SparkSession): DataFrame ={

    val rdd1 = spark.sparkContext.textFile("file///f:/sparktest/movie/data.csv")
    //val rdd2 = rdd1.flatMap(_.split("\t"))
    val rdd2 = rdd1.map(line=>{
      val arr = line.split(",")
      (
        arr(0),
        arr(1),
        arr(2),
        arr(3),
        arr(4)
      )
    })

    //导入sparksession的隐式转换
    import spark.implicits._
    //将rdd转换成数据框
    val df = rdd2.toDF("movieId","title","genres","userId","rating")

    //将数据框做成临时视图
    df.createOrReplaceTempView("_movieInfo")
    df
  }

  def main(args: Array[String]): Unit = {
    var start_time =new Date().getTime

    val data = Source.fromFile("f://sparktest/movie/movies.csv").getLines().toList
    /*
    * 根据提供的电影名进行类别搜索分析
    * */
    val movie_name = "Toy Story"
    //存放查询的所有类别
    val types:ArrayBuffer[String] = ArrayBuffer()

    data.map(line=>{
      val arr = line.split(",")
      if (arr(1).contains(movie_name)){
        for (e<-arr(2).split("\\|")){
          if(!isExist(types,e)){
            types.append(e)
          }
        }
      }
    })

    types.foreach(println)
    //println(types(0))

    //println(types.length)

    val spark = SparkSession.builder().appName("sparkSQL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()

    /*
    * 查询所有和类别相关的电影  ,按照平均评分进行降排，且评论数大于100
    * */
    val df = init(spark)
    spark.sql("select title , count(userId) as sum_comments ," +
      " round(sum(rating)/count(*),2) as avg_rating from _movieInfo " +
      "where genres like '%"+types(0)+"%' " +
      //"or genres like '%Animation%' " +
      "group by title " +
      " order by avg_rating desc"
      ).where("sum_comments>100").write.json("f:/sparktest/movie/similarMovie")
      //.show(100,false)


    var end_time =new Date().getTime
    println("总时长:"+(end_time - start_time))
  }
}
