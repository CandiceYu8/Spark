import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class CreateScore {
    private static void createScore(String infile, String outfile) {
        SparkConf conf = new SparkConf().setAppName("create scores");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Random rand = new Random();
        JavaRDD<String> input = sc.textFile(infile);
        JavaPairRDD<String, Integer> scores = input.flatMap(id->Arrays.asList(id.split(" ")).iterator())
                .mapToPair(stuId -> new Tuple2<>(stuId, rand.nextInt(100)))
                .reduceByKey((a,b) -> a + b);
        scores.saveAsTextFile(outfile);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: createScore <in> <out>");
            System.exit(2);
        }
        createScore(args[0], args[1]);
    }
}

