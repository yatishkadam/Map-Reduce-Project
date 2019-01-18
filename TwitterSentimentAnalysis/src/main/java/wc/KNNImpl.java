package wc;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNImpl
{

	// WritableComparable class for a paired Double and String (distance and model)
	// This is a custom class for MapReduce to pass a double and a String
		public static class DoubleString implements WritableComparable<DoubleString>
		{
			private Double distance = 0.0;
			private String model = null;

			public void set(Double lhs, String rhs)
			{
				distance = lhs;
				model = rhs;
			}

			public Double getDistance()
			{
				return distance;
			}

			public String getModel()
			{
				return model;
			}

			@Override
			public void readFields(DataInput in) throws IOException
			{
				distance = in.readDouble();
				model = in.readUTF();
			}

			@Override
			public void write(DataOutput out) throws IOException
			{
				out.writeDouble(distance);
				out.writeUTF(model);
			}

			@Override
			public int compareTo(DoubleString o)
			{
				return (this.model).compareTo(o.model);
			}
		}

	public static class KnnMapper extends Mapper<Object, Text, Text, DoubleString>
	{

		List<String> trainingSet = new ArrayList<>();
		String[] lines;

		// Declaring some variables which will be used throughout the mapper
		int K;


		// Takes two strings and simply compares then to return a double of 0.0 (non-identical) or 1.0 (identical).
		private double nominalDistance(String t1, String t2)
		{
			if (t1.equals(t2))
			{
				return 0;
			}

			else

			{
				return 1;
			}
		}


		private double totalSquaredDistance(double userID,double uID,String Location,String loc,
				String bookTitle,String book,String bookAuthor,String author,int yearOfPub,
				int year,String publisher,String pub)
		{
			double uidDifference = userID - uID;
			double locDifference = nominalDistance(Location,loc);
			double titleDifference = nominalDistance(bookTitle, book);
			double authorDifference = nominalDistance(bookAuthor, author);
			double publisherDifference = nominalDistance(publisher, pub);

			// The sum of squared distances
			return squaredDistance(uidDifference) + (locDifference)+ titleDifference
					+ authorDifference +  publisherDifference;
		}

		// Takes a double and returns its squared value.
		private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			 Configuration conf = context.getConfiguration();
		        K = conf.getInt("K_CONF", 5);

		        try
				{

		        String line = "";


	            URI[] cacheFiles = context.getCacheFiles();

	            for(int i=0; i<cacheFiles.length; i++)
	            {

				 URI cacheFile = cacheFiles[i];

	        	 FileSystem fs = FileSystem.get(cacheFile, new Configuration());
	        	 InputStreamReader inputStream = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
	             BufferedReader reader = new BufferedReader(inputStream);

	             try
	                {
	                    while ((line = reader.readLine()) != null)
	                    {

			        		lines = line.split("\\r?\\n");

			        		for(String l : lines)
							{
								trainingSet.add(l);
							}

	                    }
	                }

	                finally
	                {
	                	reader.close();
	                }
	            }
	        }

		    catch (IOException e)

		        {

		        }
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			TreeMap<Double, DoubleString> KnnMapp = new TreeMap<Double, DoubleString>();

			Text word = new Text();

			String rLine = value.toString();

			String[] token = rLine.split("\\|");


			double userID = Double.parseDouble(token[0]);


			String Location = token[1];


			String bookTitle = token[4];


			String bookAuthor = token[5];


			int yearOfPub =0;
			try

			{

				yearOfPub = Integer.parseInt(token[6]);
			}
			catch(NumberFormatException ex)
			{

			}
			String publisher = token[7];

			for (String line : trainingSet)
			{
				String[] tokens = line.split("\\|");

				int year =0;
				try
				{
				 yearOfPub = Integer.parseInt(token[6]);
				}
				catch(NumberFormatException ex)

				{

				}
				double tDist = totalSquaredDistance(userID, Double.parseDouble(tokens[0]), Location, tokens[1],
						bookTitle, tokens[4], bookAuthor, tokens[5], yearOfPub,
						year, publisher, tokens[7]);
				String val = tokens[4].concat(",").concat(tokens[6]).concat(",").concat(publisher);


				DoubleString distanceAndModel = new DoubleString();

					distanceAndModel.set(tDist, val);

					KnnMapp.put(tDist, distanceAndModel);


				if (KnnMapp.size() > K)
				{
					KnnMapp.remove(KnnMapp.lastKey());

				}
			}


			for (DoubleString str : KnnMapp.values())
			{

				context.write(new Text(bookTitle),str);
			}

		}

	}

	public static class KnnReducer extends Reducer<Text, DoubleString, Text, Text>
	{
		TreeMap<Double, String> KnnMapper = new TreeMap<Double, String>();
		int k;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{

			 Configuration conf = context.getConfiguration();
		     k = conf.getInt("K_CONF", 5);
		}

		@Override
		public void reduce(Text key, Iterable<DoubleString> values, Context context) throws IOException, InterruptedException
		{

			for (DoubleString val : values)
			{
				String rModel = val.getModel();
				double tDist = val.getDistance();

				KnnMapper.put(tDist, rModel);
				if (KnnMapper.size() > k)
				{
					KnnMapper.remove(KnnMapper.lastKey());
				}
			}


		    for(Map.Entry<Double, String> entry: KnnMapper.entrySet())
		    {
				context.write(new Text(entry.getKey().toString()), new Text(entry.getValue()));	// Use this line to see all K nearest neighbours and distances

		    }
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		if (args.length != 4)
		{
			System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
			System.exit(2);
		}

		conf.set("K_CONF",args[3]);
		Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
		job.setJarByClass(KNNImpl.class);


		Path path = new Path(args[2]);
        FileSystem fs = FileSystem.get(new URI(args[2]), new Configuration());
        FileStatus[] fileStat = fs.listStatus(path);
        for(FileStatus f : fileStat) {
            job.addCacheFile(f.getPath().toUri());
        }

		job.setMapperClass(KnnMapper.class);
		job.setReducerClass(KnnReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
