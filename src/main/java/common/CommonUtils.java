package common;

/**
 * Created by murali on 16/8/17.
 */
public class CommonUtils {
    public static String inputFile = "data/iris.csv";
    public static String outputFile = "data/iris_out.csv";


    public static boolean isNumeric(String str){
        return str.matches("[a-z]");
    }
}
