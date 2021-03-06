package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import weka.*;

public class Preprocessing extends Configured implements Tool {
	
		private static final Logger logger = LogManager.getLogger(Preprocessing.class);

		public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
			private final static IntWritable one = new IntWritable(1);
			private final Text word = new Text();

			@Override
			public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
				
				String[] tokens = value.toString().split("\\r?\\n");
				String tweetVal = tokens[6].concat(",").concat(tokens[3]);
				word.set(tweetVal);
	//			word.set(tokens[3]);
				context.write(new Text(tokens[5]),word);

			}
		}

//		public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
//			
//			private final IntWritable result = new IntWritable();
//
//			@Override
//			public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
//			
//
//				
//			}
//		}

		@Override
		public int run(final String[] args) throws Exception {
		
			final Configuration conf = getConf();
			final Job job = Job.getInstance(conf, "Preprocessing");
			job.setJarByClass(Preprocessing.class);
			
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");
			job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap",12);
			job.setNumReduceTasks(0);
			
			
			job.setMapperClass(TokenizerMapper.class);
//			job.setCombinerClass(IntSumReducer.class);
//			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			 job.setInputFormatClass(NLineInputFormat.class);			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			return job.waitForCompletion(true) ? 0 : 1;
		}

		public static void main(final String[] args) {
		
			if (args.length != 2) {
				throw new Error("Two arguments required:\n<input-dir> <output-dir>");
			}

			try {
			
				ToolRunner.run(new Preprocessing(), args);
			}
			catch (final Exception e) {
			
				logger.error("", e);
			}
		}
}