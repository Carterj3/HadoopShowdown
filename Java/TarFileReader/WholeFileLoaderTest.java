import static org.junit.Assert.*;

import org.apache.pig.data.Tuple;
import org.junit.Test;


// http://arunxjacob.blogspot.com/2010/12/writing-custom-pig-loader.html
public class WholeFileLoaderTest {
	 @Test
	 public void testValidInput() throws Exception{
	  WholeFileRecordReader reader = new WholeFileRecordReader("Header.tar");
	  
	  WholeFileLoader custLoader = new WholeFileLoader();
	  
	  custLoader.prepareToRead(reader, null);
	  
	  Tuple tuple = custLoader.getNext();
	  
	  assertNotNull(tuple);
	  
	  String fileName = (String)tuple.get(0);
	  String fileContents = (String)tuple.get(1);
	  
	  System.out.println(fileName);
	  System.out.println(fileContents);
	  
	 }
}
