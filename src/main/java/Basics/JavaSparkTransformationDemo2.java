package Basics;

import common.MySparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static common.CommonUtils.inputFile;
import static common.ConnectionUtils.appName;
import static common.ConnectionUtils.sparkMaster;

/**
 *
 * Created by murali on 16/8/17.
 */
public class JavaSparkTransformationDemo2 {
    public static JavaSparkContext sparkContext= null;

    
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        
        //Setting spark context
        sparkContext= MySparkContext.getSparkContext(appName,sparkMaster);

        JavaRDD<String> data= sparkContext.textFile(inputFile);

        String header= data.first();

        JavaRDD<String> irisData= data.filter(d -> !d.equals(header));

        /*
            Using Lambda expression.
         */
        JavaRDD<Sepal> sepalObject= irisData.map(s -> {
            String[] element = s.split(",");
            return new Sepal(Float.valueOf(element[0]), Float.valueOf(element[1]), Float.valueOf(element[2]),
                    Float.valueOf(element[3]), element[4]);
        }).filter(sepal -> sepal.getSpecies().equals("setosa"));

        /*

         //Using Function implementation.
        JavaRDD<Sepal> sepalObject= irisData.map(new Function<String, Sepal>() {
            @Override
            public Sepal call(String s) throws Exception {

                    String[] element = s.split(",");
                    Sepal sepal = new Sepal(Float.valueOf(element[0]), Float.valueOf(element[1]), Float.valueOf(element[2]),
                            Float.valueOf(element[3]), element[4]);
                    return sepal;

            }
        }).filter(new Function<Sepal, Boolean>() {
            @Override
            public Boolean call(Sepal sepal) throws Exception {
                return sepal.getSpecies().equals("setosa");
            }
        });
*/

        //Just print first 4 records.
        for(Sepal sepal: sepalObject.take(4)){
            System.out.println(sepal.getSepalLength() + " : " + sepal.getSpecies());
        }

        //Close the spark Context
        sparkContext.close();

    }
}


class Sepal implements Serializable{
    public float sepalLength;
    public float sepalWidth;
    public float petalWidth;
    public float petalLength;
    public String species;

    public Sepal(float sepalLength, float sepalWidth,float petalLength,float petalWidth, String species) {
        this.sepalLength = sepalLength;
        this.sepalWidth = sepalWidth;
        this.petalWidth = petalWidth;
        this.petalLength = petalLength;
        this.species = species;
    }

    public float getSepalLength() {
        return sepalLength;
    }

    public void setSepalLength(float sepalLength) {
        this.sepalLength = sepalLength;
    }

    public float getSepalWidth() {
        return sepalWidth;
    }

    public void setSepalWidth(float sepalWidth) {
        this.sepalWidth = sepalWidth;
    }

    public float getPetalWidth() {
        return petalWidth;
    }

    public void setPetalWidth(float petalWidth) {
        this.petalWidth = petalWidth;
    }

    public float getPetalLength() {
        return petalLength;
    }

    public void setPetalLength(float petalLength) {
        this.petalLength = petalLength;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }
}