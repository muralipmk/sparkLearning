package Broadcast_Accumulator;

import common.MySparkContext;
import common.MySparkPrinter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;


import static common.CommonUtils.inputFile;
import static common.ConnectionUtils.appName;
import static common.ConnectionUtils.sparkMaster;

/**
 * Find the number of records in irisRDD,
 * whose Sepal.Length is greater than the Average Sepal Length we found in the earlier practice
 * Created by murali on 17/8/17.
 */
public class BroadAccDemo {
    public static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        MySparkPrinter printer = new MySparkPrinter();

        sparkContext = MySparkContext.getSparkContext(appName, sparkMaster);

        //Load the data to the RDD.
        JavaRDD<String> data = sparkContext.textFile(inputFile);

        LongAccumulator count= sparkContext.sc().longAccumulator();

        JavaRDD<Float> irisRDD= data.map(s -> {
                String[] ele= s.split(",");
                return ele[0].equals("SepalLength") ? new Float(0.0) : Float.valueOf(ele[0]);
        });
        float total_sum= irisRDD.reduce((a,b) -> a + b);

        Broadcast<Float> average= sparkContext.broadcast(new Float(total_sum / irisRDD.count()));

        JavaRDD<Float> rdd= irisRDD.map(f -> {
            if(f > average.getValue())
                count.add(1);
            else count.add(0);
            return f;
        });


    }
}