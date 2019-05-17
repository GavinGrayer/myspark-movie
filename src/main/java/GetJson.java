import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ClassName GetJson
 * Author    MxRanger
 * Date      2019/4/12
 * Time      9:20
 */

public class GetJson {

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("sparkSQL")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> comments = SparkSQLTMovieDemo.comments(spark);
        comments.toJSON().foreach(e->{
            System.out.println(e);
        });

    }
}
