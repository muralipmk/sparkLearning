package KeyValue;

import common.MySparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import static common.CommonUtils.inputFile;
import static common.ConnectionUtils.appName;
import static common.ConnectionUtils.sparkMaster;

/**
 *
 * Created by murali on 17/8/17.
 */
public class KeyValuePairDemo {
    static JavaSparkContext sparkContext= null;

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        sparkContext= MySparkContext.getSparkContext(appName,sparkMaster);

        JavaRDD<String> irisRDD= sparkContext.textFile(inputFile);

        //Using lambda expression.
        JavaPairRDD<String,Double> irisKeyPair= irisRDD.mapToPair(s -> {
            String[] str = s.split(",");
            return new Tuple2<String, Double>(str[4], str[0].equals("SepalLength") ? new Double(0.0) : Double.valueOf(str[0]));
        }).reduceByKey((d1, d2) -> d1 < d2 ? d1 : d2);

        //Using PairFunction implementation
        /*
        JavaPairRDD<String,Double> irisKeyPair= irisRDD.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] str= s.split(",");
                return new Tuple2<String, Double> (str[4],str[0].equals("SepalLength") ? new Double(0.0) : Double.valueOf(str[0]));
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble < aDouble2 ? aDouble : aDouble2;
            }
        });*/

        for(Tuple2<String,Double> item: irisKeyPair.take(4)){
            System.out.println(item);
        }
    }
}
