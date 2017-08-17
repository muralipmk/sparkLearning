package common;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by murali on 16/8/17.
 */
public class MySparkPrinter<T> {
    public T element;

    public void printfRdd_take(JavaRDD<T> rdd, int num){
        for(T elemt: rdd.take(num))
            System.out.println(elemt.toString());
    }

    public void printRdd(JavaRDD<T> rdd){
        for(T elemt: rdd.collect()){
            System.out.println(elemt.toString());
        }
    }
}
