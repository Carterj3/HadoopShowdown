import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class ScriptMain {

	public static void main(String[] args) throws IOException, URISyntaxException {
		
		// put <file> <where>
		if (args.length == 3) {
			String server = args[0];
			String local = args[1];
			String remote = args[2];
			
			// https://stackoverflow.com/questions/11041253/set-hadoop-system-user-for-client-embedded-in-java-webapp
			System.setProperty("HADOOP_USER_NAME", "hdfs");
			
			// http://stackoverflow.com/questions/16000840/write-a-file-in-hdfs-with-java
			String url = "hdfs://"+server+"/"+remote;
			Path pt=new Path(url);
            FileSystem fs = FileSystem.get(new URI(url),new Configuration());
            
            FSDataOutputStream file = fs.create(pt);
            FileInputStream input = new FileInputStream(new File(local));
            
            int bytesRead = 0;
            byte[] buffer = new byte[100];
            
            while ((bytesRead = input.read(buffer)) > 0) {
            	  file.write(buffer, 0, bytesRead);
            	}
            
            file.close();
            input.close();

		} else
		// err
		{
			System.out.println("Incorrect usage of the application");
			String flattenedString = "";
			for(String s : args){
				flattenedString = flattenedString+","+s;
			}
			System.out.println("You passed ["+args.length+"]: "+flattenedString.substring(1));
	        System.out.println("Correct args are '<server> <local file> <remote loc>'");
	        
		}
	}

}
