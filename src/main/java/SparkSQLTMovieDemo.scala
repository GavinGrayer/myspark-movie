import java.util.Date

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * ClassName SparkSQLDemo1
  * Author    MxRanger
  * Date      2019/4/5
  * Time      9:40
  *
  * scala实现sparksql  wordcount
  *
  **/

object SparkSQLTMovieDemo {

  /*
  *  [1]电影评论数排名
  * */
  def comments(spark:SparkSession): DataFrame ={

    init(spark)

    val comments_sql = "select  title , count(userId) as sum_comments " +
      "from _movieInfo" +
      " group by title" +
      " order by count(userId) desc"
    //spark.sql(comments_sql).write.json("F:\\sparktest\\movie\\comments")
    spark.sql(comments_sql)
  }

  /*
  *  [2]所有电影平均评分的排名 且评论人数超过100人
  * */
  def sum_rating(spark: SparkSession): DataFrame ={

    init(spark)


    val sum_rating_sql = "select  title , count(userId) as sum_comments ," +
      " round(sum(rating)/count(*),2) as avg_rating " +
      "from _movieInfo" +
      " group by title" +
      " order by avg_rating desc"

    //spark.sql(sum_rating_sql).where("sum_comments > 150").write.json("f:/sparktest/movie/avg_rating")
    spark.sql(sum_rating_sql)
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

    val spark = SparkSession.builder().appName("sparkSQL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport()
      .getOrCreate()

    /*
    *  [1]电影评论数排名
    * */
    comments(spark).show(20,false)
    comments(spark).write.json("f:/sparktest/movie/comments")
    /*
    *  [2]所有电影平均评分的排名 且评论人数超过150人
    * */
    sum_rating(spark).show(20,false)
    sum_rating(spark).where("sum_comments > 150").write.json("f:/sparktest/movie/ag_rating")

    /*
    *  [3]查找类似类别电影
    * */

    var end_time =new Date().getTime
    println("总时长:"+(end_time - start_time))
  }

}
