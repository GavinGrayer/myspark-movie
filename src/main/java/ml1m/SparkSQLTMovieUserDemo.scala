package ml1m

import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * ClassName SparkSQLDemo1
  * Author    MxRanger
  * Date      2019/4/5
  * Time      9:40
  *
  * scala实现sparksql  用户分析
  *
  **/

object SparkSQLTMovieUserDemo {

  /*
  *  [1]不同职业排行
  * */
  def occupations(spark:SparkSession): DataFrame ={

    init(spark)

    val comments_sql = "select Occupation, count(Occupation)  " +
      "from _userInfo " +
      " group by Occupation" +
      " order by count(Occupation) desc"
    //spark.sql(comments_sql).write.json("F:\\sparktest\\movie\\comments")
    spark.sql(comments_sql)
  }

  /*
*  [1]不同职业排行
* */
  def ageRange(spark:SparkSession): DataFrame ={

    init(spark)

    val comments_sql = "select Age, count(Age)  " +
      "from _userInfo " +
      " group by Age" +
      " order by count(Age) desc"
    //spark.sql(comments_sql).write.json("F:\\sparktest\\movie\\comments")
    spark.sql(comments_sql)
  }


  def init(spark:SparkSession): DataFrame ={

    val occupations:Map[Int,String] = Map (0->"其他",
     1->"学术/教育者", 2->"艺术家", 3->"牧师/管理员", 4->"大学/研究生", 5->"客户服务",
     6->"医生/保健", 7->"执行/管理", 8->"农民", 9->"家庭主妇", 10->"K-12学生",
     11->"律师", 12->"程序员", 13->"退休", 14->"销售/营销", 15->"科学家", 16->"自雇人士",
     17->"技师/工程师", 18->"匠人/工匠", 19->"失业", 20->"作家")

    val ageRange:Map[Int,String] = Map(1->"18岁以下",
      18->"18-24", 25->"25-34", 35->"35-44", 45->"45-49", 50->"50-55", 56->"56+")

    val rdd1 = spark.sparkContext.textFile("file///f:/sparktest/recommend/user_info.csv")
    //val rdd2 = rdd1.flatMap(_.split("\t"))
    val rdd2 = rdd1.map(line=>{
      val arr = line.split(";")
      var flag = 0
        (
          arr(0),
          arr(1),
          ageRange.get(arr(2).toInt),
          occupations.get(arr(3).toInt)
        )
    })

    //导入sparksession的隐式转换
    import spark.implicits._
    //将rdd转换成数据框
    val df = rdd2.toDF("UserID","Gender","Age","Occupation")

    //将数据框做成临时视图
    df.createOrReplaceTempView("_userInfo")
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
    *  [1] [1]不同职业排行
    */
    occupations(spark).show(20,false)
    occupations(spark).write.json("f:/sparktest/recommend/ml1mresult/occupations")


    ageRange(spark).show(20,false)
    ageRange(spark).write.json("f:/sparktest/recommend/ml1mresult/ageRange")

    var end_time =new Date().getTime
    println("总时长:"+(end_time - start_time))
}

}
