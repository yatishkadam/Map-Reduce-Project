package kMeans;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class kMeansProg extends Configured implements Tool {
    private static String SPLITTER = "\t| ";
	private static final Logger logger = LogManager.getLogger(kMeansProg.class);
	private static final int max = 564512; // Max to be set after finding the max value.

	public static class MapperClass extends  Mapper<Object, Text, DoubleWritable, DoubleWritable> {
        private final List<Double> centroidsCenters = new ArrayList<Double>();
        private final DoubleWritable val1 = new DoubleWritable();
        private final DoubleWritable val2 = new DoubleWritable();


        //setup and get the cached files from cache for all the mappers.
        @Override
        public void setup(Context context) throws IllegalArgumentException, IOException {
            try {
                URI[] cacheFiles = context.getCacheFiles();
                centroidsCenters.clear();
                for(int i=0; i < cacheFiles.length; i++) {
                    String line;
                    URI cacheFile = cacheFiles[i];
                    FileSystem fs = FileSystem.get(cacheFile, new Configuration());
                    InputStreamReader streamReader = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
                    try(BufferedReader reader = new BufferedReader(streamReader)) {
                        while ((line = reader.readLine()) != null) {
                            centroidsCenters.add(Double.parseDouble(line.split(SPLITTER)[0]));
                            System.out.println(centroidsCenters);
                        }
                    }
                }
            } catch (IOException e) {
                logger.info(e);
            }
        }

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] lineSplit = line.split(SPLITTER);
            Double point = Double.parseDouble(lineSplit[1]);

            double closestCentre = centroidsCenters.get(0);
            double minDistance = Math.abs(point - closestCentre);

            for(int i =0;i < centroidsCenters.size();i++)
            {
                if( Math.abs(point - centroidsCenters.get(i)) < minDistance)
                {
                    closestCentre = centroidsCenters.get(i);
                    minDistance = Math.abs(point - centroidsCenters.get(i));

                }
            }

            val1.set(closestCentre);
            val2.set(point);

            context.write(val1,val2);
        }
	}

	public static class ReducerClass extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();
        private final DoubleWritable sseResult = new DoubleWritable();
        private final List<Double> points = new ArrayList<Double>();

        @Override
		public void reduce(final DoubleWritable key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            int sse = 0 ;
            double newCenter;
            double sum = 0;
            int no_elements = 0;
            for (final DoubleWritable v : values) {
                double d = v.get();
                sum = sum + d;
                ++no_elements;
                points.add(d);
            }


            // We have new center now
            newCenter = sum / no_elements;
            for(int i =0 ;i<points.size();i++){
                sse += Math.pow((newCenter - points.get(i)), 2);
            }
            // Emit new center and point
            result.set(newCenter);
            sseResult.set(sse);
            context.write(result, sseResult );
		}
	}


    /**
     * This is used to save all the data to the cache system.
     * @param path - the path of the folder
     * @param job - the job
     * @throws Exception
     */
    public void saveToCache(String path,Job job) throws Exception {
        FileSystem fs = FileSystem.get(new URI(path), new Configuration());
        FileStatus[] fileStatus = fs.listStatus(new Path(path));
        for(FileStatus file : fileStatus) {
            job.addCacheFile(file.getPath().toUri());
        }
    }


    /**
     * This fucntion is used to cal the centroid for the inital start of the program.
     * @param k - the nmber of centroid to start with
     * @param centriodPath - the centroid initial directory
     * @throws IOException
     */
    public void makeInitialKNodes(int k,String centriodPath) throws IOException {
	    double someDiv = max/k;
	    List<Double> ls = new ArrayList<>();
	    for(int i =0;i<k;i++){
	        ls.add(someDiv *(i+1));
        }

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(centriodPath+"/centroid.txt"), "utf-8"))) {

            for(Double d: ls) {
                writer.write(d+"");
                writer.write("\n");

            }
        }
    }

	@Override
	public int run(final String[] args) throws Exception {
	    String OUT = args[1]+"/output";
        String output = OUT + System.nanoTime();
        String again_input = output;


	    String in = args[0];
	    String centroid = args[2];
	    String numCentroid = args[3];
	    final Configuration conf = getConf();


        for(int i = 0; i<10; i++ ){

            if (i == 0) {
               makeInitialKNodes(Integer.parseInt(numCentroid), centroid);
            } else {
                centroid = again_input;
            }
            final Job job = Job.getInstance(conf, "kMeans");
            job.setJarByClass(kMeansProg.class);
            final Configuration jobConf = job.getConfiguration();

            saveToCache(centroid,job);
            jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
            job.setMapperClass(MapperClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);

            again_input = output;
            output = OUT + System.nanoTime();
        }

        return 0;
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir> <centroid-dir> <k>");
		}

		try {
			ToolRunner.run(new kMeansProg(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}