
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by 11130 on 13/02/16.
 */
public class MinTemperature {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("MinimumTemperature").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/Weather_data.csv");

        JavaRDD<Tuple3<String, String, Double> > parsedLines = lines.map(
                s -> {
                    String[] fields = s.split(",");
                    String stationID = fields[0];
                    String entryType = fields[2];
                    Double temperature = Double.parseDouble(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0;

                    return new Tuple3<>(stationID, entryType, temperature);
                }
        );


        JavaRDD<Tuple3<String, String, Double> > minTemps = parsedLines.filter(
                s -> (s._2().equals("TMIN"))
        );

        JavaPairRDD<String, Double> stationTemps = minTemps.mapToPair(
                s -> new Tuple2<>(s._1(), s._3())
        );

        JavaPairRDD<String, Double> minTemps1 = stationTemps.reduceByKey(
                (x, y) -> (x < y) ? x : y
        );

        Map<String, Double> result = new HashMap<>(minTemps1.collectAsMap());

        for (Map.Entry<String, Double> m : result.entrySet()) {
            System.out.println(m.getKey() + " " + (double)Math.round(m.getValue() * 100.00) / 100.00 + "F");
        }
    }
}
