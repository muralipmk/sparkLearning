package Actions;

import common.MySparkContext;
import common.MySparkPrinter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import static common.CommonUtils.inputFile;
import static common.ConnectionUtils.appName;
import static common.ConnectionUtils.sparkMaster;

/**
 * It demonstrate spark basic actions.(count,first,take,reduce)
 * Created by murali on 16/8/17.
 */
public class SparkBActions {
    public static JavaSparkContext sparkContext= null;

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        MySparkPrinter printer= new MySparkPrinter();

        sparkContext= MySparkContext.getSparkContext(appName,sparkMaster);

        //Load the data to the RDD.
        JavaRDD<String> data= sparkContext.textFile(inputFile);

        String header= data.first();

        //Using lambda expression.
        JavaRDD<Float> sepal= data.map(s -> !s.equals(header) ? Float.valueOf(s.split(",")[0]) : new Float(0.0));

        //Using Function implementation
        /*
        JavaRDD<Float> sepal= data.map(new Function<String, Float>() {
            @Override
            public Float call(String s) throws Exception {
                return !s.equals(header) ? Float.valueOf(s.split(",")[0]) : new Float(0.0);
            }
        });
        */

        printer.printfRdd_take(sepal,4);
        //Find the total of sepal length. (Inline function)
        float total_Sum= sepal.reduce((a,b) -> a + b);
        float average= total_Sum / sepal.count();
        //Average of sepal length.
        System.out.println("Average of Sepal Length: " + average);

    }
}
