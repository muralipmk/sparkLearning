package Spark_Streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Demonstration of word using spark streaming.
 * Created by murali on 17/8/17.
 */
public class WordCount {

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        //Set up the spark configuration.
        SparkConf sparkConf= new SparkConf().setAppName("SparkWordStream").setMaster("local[2]");

        //Setup java spark streaming context
        JavaStreamingContext streamingContext= new JavaStreamingContext(sparkConf, Durations.seconds(3));

        //Setup Input java Dstream.
        JavaReceiverInputDStream<String> line= streamingContext.socketTextStream("localhost",1234);

        //Create Dstream of words using the flatMap.
        JavaDStream<String> words= line.flatMap(w -> Arrays.asList(w.split(" ")).iterator());

        JavaPairDStream<String, Integer> word_count= words.mapToPair(wc -> new Tuple2<>(wc,1));
        JavaPairDStream<String, Integer> stream_count= word_count.reduceByKey((wc1,wc2) -> wc1 + wc2);

        //Print the Dstream work count.
        stream_count.print();

        //Sliding window demonstration.
        JavaPairDStream<String,Integer> windowWordCount= word_count.reduceByKeyAndWindow((ws1, ws2) -> ws1 + ws2, Durations.seconds(15),Durations.seconds(3));

        System.out.println("Window stream count");
        windowWordCount.print();

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        streamingContext.close();

    }
}
