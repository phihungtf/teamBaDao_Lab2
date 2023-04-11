import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {
	// mapper
	public static class MaxTempMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// get the necessary fields from the input record
			String[] fields = value.toString().split("\\s+");
			String year = fields[0];
			String temp = fields[1];
			
			// emit the year and the temperature
			context.write(new IntWritable(Integer.parseInt(year)),
						  new FloatWritable(Float.parseFloat(temp)));
		}
	}

	// reducer
	public static class MaxTempReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			// find the maximum temperature
			float maxTemp = Float.MIN_VALUE;
			for (FloatWritable value : values) {
				if (value.get() > maxTemp) {
					maxTemp = value.get();
				}
			}
			
			// emit the year and the maximum temperature
			context.write(key, new FloatWritable(maxTemp));
		}
	}

	// main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MaxTemp");

		// if the output path already exists, delete it
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		// set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// set the mapper and reducer classes
		job.setJarByClass(MaxTemp.class);
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		
		// set the output key and value classes
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		
		// submit the job and wait for its completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
