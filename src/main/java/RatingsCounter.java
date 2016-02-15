import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by 11130 on 13/02/16.
 */
public class RatingsCounter {
    public static void main(String args[]) {

        SparkConf conf = new SparkConf().setAppName("RatingsHistogram").setMaster("local[*]");  //Creating a Spark Conf
        JavaSparkContext sc = new JavaSparkContext(conf);        //Making JavaSparkContext
        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/ml-100k/u.data");   //Making a JavaRDD of type String

        //This is using Spark Java APIs
//      JavaRDD<String> ratings = lines.map(new Function<String, String>() {
//            public String call(String s) throws Exception {
//                return s.split("\t")[2];
//            }
//       });


        //This is using Lambda expressions
        JavaRDD<String> ratings = lines.map(
                s -> s.split("\t")[2]
        );

        Map<String, Long> result = new TreeMap<>(ratings.countByValue());

        for (Map.Entry<String, Long> entry : result.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
