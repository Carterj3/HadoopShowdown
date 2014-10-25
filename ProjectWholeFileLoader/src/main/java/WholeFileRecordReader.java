

//cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
class WholeFileRecordReader extends RecordReader<Text, Text> {

private FileSplit fileSplit;
private Configuration conf;
private Text value = new Text();
private boolean processed = false;
private TaskAttemptContext context;

@Override
public void initialize(InputSplit split, TaskAttemptContext context)
   throws IOException, InterruptedException {
 this.fileSplit = (FileSplit) split;
 this.conf = context.getConfiguration();
 this.context = context;
}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException {
 if (!processed) {
   byte[] contents = new byte[(int) fileSplit.getLength()];
   Path file = fileSplit.getPath();
   FileSystem fs = file.getFileSystem(conf);
   FSDataInputStream in = null;
   try {
     in = fs.open(file);
     IOUtils.readFully(in, contents, 0, contents.length);
     String str_value = new String(contents,"UTF-8");
     value.set(str_value);
     
   } finally {
     IOUtils.closeStream(in);
   }
   processed = true;
   return true;
 }
 return false;
}

@Override
public Text getCurrentKey() throws IOException, InterruptedException {
 return new Text(this.fileSplit.getPath().toString());
}

@Override
public Text getCurrentValue() throws IOException,
   InterruptedException {
 return value;
}

@Override
public float getProgress() throws IOException {
 return processed ? 1.0f : 0.0f;
}

@Override
public void close() throws IOException {
 // do nothing
}
}
//^^ WholeFileRecordReader