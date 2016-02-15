import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 11130 on 14/02/16.
 */
public class PopularMoviesNicer {

    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Popular Movies Nicer").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        class MvDict {
            Map<Integer, String> getMovieDict() {
                Map<Integer, String> movieDict = new HashMap<>();
                BufferedReader br = null;

                try {

                    String sCurrentLine;

                    br = new BufferedReader(new FileReader("/Users/11130/udemy/SparkCourse/ml-100k/u.item"));

                    while ((sCurrentLine = br.readLine()) != null) {
                        String str = sCurrentLine;
                        String[] fields = str.split("\\|");

                        movieDict.put(Integer.parseInt(fields[0]), fields[1]);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

                return movieDict;
            }
        }

        Broadcast<Map<Integer, String> > movieDict = sc.broadcast(new MvDict().getMovieDict());

        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/ml-100k/u.data");
        JavaPairRDD<String, Integer> movieId = lines.mapToPair(
                s -> new Tuple2<>(s.split("\t")[1], 1)
        );

        JavaPairRDD<String, Integer> countOfMovies = movieId.reduceByKey(
                (x, y) -> x + y
        );

        JavaPairRDD<Integer, String> flippedCountOfMovies = countOfMovies.mapToPair(
                s -> new Tuple2<Integer, String>(s._2(), s._1())
        ).sortByKey();

        JavaPairRDD<String, Integer> countAndNames = flippedCountOfMovies.mapToPair(
                s -> new Tuple2<>(movieDict.getValue().get(Integer.parseInt(s._2())), s._1())
        );

        List<Tuple2<String, Integer> > result = new ArrayList<>(countAndNames.collect());

        for(Tuple2<String, Integer> t: result) {
            System.out.println(t._1() + " -------  " + t._2());
        }


    }
}
