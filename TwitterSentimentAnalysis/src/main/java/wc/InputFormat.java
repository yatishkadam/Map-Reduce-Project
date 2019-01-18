package wc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

/**
 * Reads records that are delimited by a specifc begin/end tag.
 */
public class InputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
			throws IOException {
		return new XmlRecordReader((FileSplit) inputSplit, jobConf);
	}

	public static class XmlRecordReader implements RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			startTag = "***".getBytes("utf-8");
			endTag = "***".getBytes("utf-8");

			// open the file and seek to the start of the split
			start = split.getStart();
			end = start + split.getLength();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(jobConf);
			fsin = fs.open(split.getPath());
			fsin.seek(start);
		}
		
		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}


		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(fsin.getPos());
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) 
			{
				int b = fsin.read();
				if (b == -1)
					return false;
				if (withinBlock)
					buffer.write(b);
				if (b == match[i]) 
				{
					i++;
					if (i >= match.length)
						return true;
				} 
				else
					i = 0;
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
		
		

		@Override
		public Text createValue()
		{
			return new Text();
		}

		@Override
		public long getPos() throws IOException
		{
			return fsin.getPos();
		}
		

		@Override
		public void close() throws IOException
		{
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException
		{
			return ((fsin.getPos() - start) / (float) (end - start));
		}
		
	}
}