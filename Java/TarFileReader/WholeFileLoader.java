import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

// WholeFileLoader is used to read a whole archive (tar,bzip2) as a single record.

/*
 This is a sample pig script that uses it.
 ## 
 
pig -param ant=hdfs:///tmp/lib/ant-1.9.4.jar -param input=/tmp/Showdown/Exp/Header.tar -param loader=hdfs:///tmp/lib/TarFileReader-0.0.1-SNAPSHOT.jar tar.pig
 
register '$ant'
register '$loader'

input_data = load '$input' USING WholeFileLoader() AS (filename:chararray,text:chararray);
dump input_data;

 */

public class WholeFileLoader extends LoadFunc {

	private RecordReader reader;
	TupleFactory tupleFactory = TupleFactory.getInstance();

	public WholeFileLoader() {
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat getInputFormat() {
		return new WholeFileInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			if (!reader.nextKeyValue()) {
				return null;
			}
			
			String key = ((Text) reader.getCurrentKey()).toString();
			String value = ((Text) reader.getCurrentValue()).toString();
			
			Tuple tuple = tupleFactory.newTuple(2);

			tuple.set(0, key);
			tuple.set(1, value);

			return tuple;
		} catch (InterruptedException e) {
			throw new ExecException(e);
		}
	}
}