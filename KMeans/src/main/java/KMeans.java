import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class KMeans {
    private static final int D = 4;
    private static final int MaxIteration = 100;
    private static final double Threshold = 0.001;
    private static List<Tuple2<Double, Double>> old_center = new ArrayList<>();
    private static List<Tuple2<Double, Double>> new_center = new ArrayList<>();

    static class ParseInput implements PairFunction<String, Double, Double> {
        public Tuple2<Double, Double> call(String line) throws Exception {
            String[] tmp = line.split(",");
            Double x = Double.parseDouble(tmp[0]);
            Double y = Double.parseDouble(tmp[1]);
            return new Tuple2<>(x, y);
        }
    }

    static class AddKey implements PairFunction<Tuple2<Double, Double>, Integer, Tuple2<Tuple2<Double, Double>, Integer>> {
        public Tuple2<Integer, Tuple2<Tuple2<Double, Double>, Integer>> call(Tuple2<Double, Double> line) {
            double minDis = 999999;
            double tmpX, tmpY, dis;
            int cluster = 0;
            for (int i = 0; i < D; i++) {
                tmpX = Math.pow(line._1 - old_center.get(i)._1, 2);
                tmpY = Math.pow(line._2 - old_center.get(i)._2, 2);
                dis = Math.sqrt(tmpX + tmpY);
                if (dis < minDis) {
                    minDis = dis;
                    cluster = i;
                }
            }
            return new Tuple2<>(cluster, new Tuple2<>(line, 1));
        }
    }

    private static void kMeans(JavaPairRDD<Double, Double> dots) {
        JavaPairRDD<Integer, Tuple2<Tuple2<Double, Double>, Integer>> dotGroup = dots.mapToPair(new AddKey());
        JavaPairRDD<Integer, Tuple2<Tuple2<Double, Double>, Integer>> AddGroup = dotGroup.reduceByKey(
                (val1, val2) -> new Tuple2<>(new Tuple2<>(val1._1._1 + val2._1._1, val1._1._2 + val2._1._2), val1._2 + val2._2));
        JavaPairRDD<Double, Double> centerRDD = AddGroup.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple2<Double, Double>, Integer>>, Double, Double>() {
            @Override
            public Tuple2<Double, Double> call(Tuple2<Integer, Tuple2<Tuple2<Double, Double>, Integer>> line) throws Exception {
                double x = line._2._1._1 / line._2._2;
                double y = line._2._1._2 / line._2._2;
                return new Tuple2<>(x, y);
            }
        });
        JavaPairRDD<Double, Double> sortRDD = centerRDD.sortByKey();
        new_center = sortRDD.collect();
    }


    private static boolean convergence() {
        boolean result = true;
        double errorness = 0;
        double tmpX, tmpY;
        for (int i = 0; i < D; i++) {
            tmpX = Math.pow(new_center.get(i)._1 - old_center.get(i)._1, 2);
            tmpY = Math.pow(new_center.get(i)._2 - old_center.get(i)._2, 2);
            errorness += Math.sqrt(tmpX + tmpY);
        }
        if (errorness > Threshold)
            result = false;
        return result;
    }


    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: kMeans <dots> <centers> <out>");
            System.exit(2);
        }

        SparkConf conf = new SparkConf().setAppName("kMeans");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputDots = sc.textFile(args[0]);
        JavaPairRDD<Double, Double> dots = inputDots.mapToPair(new ParseInput());
        JavaRDD<String> inputCenters = sc.textFile(args[1]);
        JavaPairRDD<Double, Double> centersRDD = inputCenters.mapToPair(new ParseInput());
        old_center = centersRDD.collect();

        int numIter = 1;
        while (true) {
            System.out.println("Iteration number: " + numIter);
            kMeans(dots);

            if (convergence() || numIter >= MaxIteration) {
                System.out.println("convergence.");
                old_center = new ArrayList<>(new_center);
                System.out.println("Total iterations: " + numIter);
                break;
            }
            numIter++;
            old_center = new ArrayList<>(new_center);
        }

        JavaPairRDD<Double, Double> result = sc.parallelizePairs(old_center);
        result.saveAsTextFile(args[2]);
    }
}

