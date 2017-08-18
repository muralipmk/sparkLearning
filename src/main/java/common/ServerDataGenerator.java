package common;

import java.io.File;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * This is the server for generating random data to be used as streaming data for the spark streaming demo.
 * Created by murali on 17/8/17.
 */
public class ServerDataGenerator {

    public static void main(String[] args){

        //Read data from the file.
        Path path = FileSystems.getDefault().getPath("data/randomdata.txt");
        PrintStream outStream= null;

        try {
            //Create a list to store the data read from the file.
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

            ServerSocket serverSocket= new ServerSocket(1234);
            Socket socket= serverSocket.accept();

            outStream= new PrintStream(socket.getOutputStream());

            while(true){
                int select_line= (int) (Math.random() * lines.size());
                outStream.println(lines.get(select_line));
                outStream.flush();
                Thread.sleep((long) (Math.random() * 3000));
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
