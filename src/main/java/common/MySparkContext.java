package common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by murali on 16/8/17.
 */
public class MySparkContext {

    public static JavaSparkContext context= null;

    public static JavaSparkContext getSparkContext(String appName, String sparkMaster){
        if(context == null){
            SparkConf sparkConf= new SparkConf().setAppName(appName).setMaster(sparkMaster);
            context= new JavaSparkContext(sparkConf);
        }
        return context;
    }
}
