import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class AvgScore {
    static class ParseLine implements PairFunction<String, Integer, Tuple2<Double, Integer>> {
        public Tuple2<Integer, Tuple2<Double, Integer>> call(String line) {
            Integer groupID = (Integer.parseInt(line.split(",")[0])-1)/5+1;
            Double score = Double.parseDouble(line.split(",")[1]);
            System.out.println(groupID + " " + score);
            return new Tuple2<>(groupID, new Tuple2<>(score, 1));
        }
    }

    static class avgByKey implements PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double> {
        public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> tupleTmp) {
            Integer id = tupleTmp._1;
            Double avg = tupleTmp._2._1/tupleTmp._2._2;
            System.out.println(id + " " + avg);
            return new Tuple2<>(id, avg);
        }
    }

    private static void avgScore(String infile, String outfile) {
        SparkConf conf = new SparkConf().setAppName("avgScore");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(infile);
        JavaPairRDD<Integer, Tuple2<Double, Integer>> group = input.mapToPair(new ParseLine());
        JavaPairRDD<Integer, Tuple2<Double, Integer>> aggregate =
                group.reduceByKey((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        JavaPairRDD<Integer, Double> avg = aggregate.mapToPair(new avgByKey());
        avg.saveAsTextFile(outfile);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: avgScore <in> <out>");
            System.exit(2);
        }
        avgScore(args[0], args[1]);
    }
}
