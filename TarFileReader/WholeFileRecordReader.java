import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

//vv WholeFileRecordReader
class WholeFileRecordReader extends RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private Configuration conf;
	
	private TaskAttemptContext context;
	
	private Text key = new Text();
	private Text value = new Text();

	private TarInputStream stream;
	
	private long Length;
	private long processed;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.context = context;

		Length = fileSplit.getLength();
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		try {
			stream = openInputFile((InputStream) fs.open(file), file.getName());
		} catch (Exception e) {
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		TarEntry entry;
		if ((entry = stream.getNextEntry()) != null) {
			while (entry.isDirectory()) {
				continue;
			}
			String filename = entry.getName();
			byte[] data = getBytes(stream, entry.getSize());

			key.set(new Text(filename));
			value.set(new Text(data));
			
			processed += data.length;

			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return processed/Length;
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}

	private static TarInputStream openInputFile(File inputFile)
			throws Exception {
		InputStream fileStream = new FileInputStream(inputFile);
		return openInputFile(fileStream, inputFile.getName());
	}

	private static TarInputStream openInputFile(InputStream fileStream,
			String name) throws Exception {
		InputStream theStream = null;
		if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
			theStream = new GZIPInputStream(fileStream);
		} else if (name.endsWith(".tar.bz2") || name.endsWith(".tbz2")) {
			fileStream.skip(2);
			theStream = new CBZip2InputStream(fileStream);
		} else {
			theStream = fileStream;
		}
		return new TarInputStream(theStream);
	}

	private static byte[] getBytes(TarInputStream input, long size) {
		if (size > Integer.MAX_VALUE) {
			return new byte[0];
		}
		int length = (int) size;
		byte[] bytes = new byte[length];

		int offset = 0;
		int numRead = 0;

		try {
			while (offset < bytes.length
					&& (numRead = input.read(bytes, offset, bytes.length
							- offset)) >= 0) {
				offset += numRead;
			}
		} catch (IOException e) {
			return bytes;
		}

		if (offset < bytes.length) {
			return new byte[0];
		}
		return bytes;
	}
}