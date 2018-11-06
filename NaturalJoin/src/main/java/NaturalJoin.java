import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class NaturalJoin {
    static class ParseAddr implements PairFunction<String, String, String> {
        public Tuple2<String, String> call(String line) {
            String[] lineParse = line.split(" ");
            if(lineParse.length != 2) {
                System.err.println("Input address.txt line error");
                System.err.println(line);
                return new Tuple2<>("-1", "-1");
            }
            return new Tuple2<>(lineParse[0], lineParse[1]);
        }
    }

    static class ParsePerson implements PairFunction<String, String, Tuple2<String, String>> {
        public Tuple2< String, Tuple2<String, String>> call(String line) {
            String[] lineParse = line.split(" ");
            if(lineParse.length != 3) {
                System.err.println("Input person.txt line error");
                System.err.println(line);
                return new Tuple2<>("-2", new Tuple2<>("-2", "-2"));
            }
            return new Tuple2<>(lineParse[2], new Tuple2<>(lineParse[0], lineParse[1]));
        }
    }

    static class ReOutput implements PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, String>>, String, String> {
        public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, String>, String>> line) {
            System.out.println(line);
            String first = line._2._1._1 + "," + line._2._1._2;
            String second = line._1 + "," + line._2._2;
            return new Tuple2<>(first, second);
        }
    }

    private static void naturalJoin(String inAddr, String inPerson, String outfile) {
        SparkConf conf = new SparkConf().setAppName("naturalJoin");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> addr = sc.textFile(inAddr);
        JavaRDD<String> person = sc.textFile(inPerson);
        JavaPairRDD<String, String> addr_new = addr.mapToPair(new ParseAddr());
        JavaPairRDD<String, Tuple2<String, String>> person_new = person.mapToPair(new ParsePerson());

        JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> joined = person_new.join(addr_new);
        JavaPairRDD<String, String> result = joined.mapToPair(new ReOutput());
        result.saveAsTextFile(outfile);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: naturalJoin <address> <person> <out>");
            System.exit(2);
        }
        naturalJoin(args[0], args[1], args[2]);
    }
}
