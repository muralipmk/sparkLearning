package Basics; /**
 * Sets JavaSpark Contexts.
 * Loads data from the local file system to spark.
 * Print num of records from the rdd.
 * Counts the number of records in the rdd.
 * @author P Murali krishna
 */

import common.CommonUtils;
import common.MySparkContext;
import common.MySparkPrinter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static common.ConnectionUtils.appName;
import static common.ConnectionUtils.sparkMaster;


public class JavaSparkDemo1 {
    public static JavaSparkContext sparkContext= null;

    public static MySparkPrinter printer= new MySparkPrinter();

    public static void main(String[] args) throws Exception {
        /*---------------------------------------------------------------------*/
        //Logging the spark info.
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        // Create a Java Spark Context.
        sparkContext = MySparkContext.getSparkContext(appName,sparkMaster);

        //Load iris.csv file to spark
        JavaRDD<String> irisData = sparkContext.textFile(CommonUtils.inputFile);
        irisData.cache(); //cached data for the later use.

        //Take num as an argument to print the first num records in rdd.
        printer.printfRdd_take(irisData,5);
        printer.printRdd(irisData);

        System.out.println("Count of records: " + irisData.count());

        //Close the spark Context
        sparkContext.close();
    }
}