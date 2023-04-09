import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import mapreduce.*;
import mapreduce.VertexReducer.JobCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class ConnectedComponents {
	// default value for input and output files
	private static final String INPUT = "hdfs://localhost:9000/user/phihungtf/cc/input.txt";
    private static final String OUTPUT = "hdfs://localhost:9000/user/phihungtf/cc/output";

	public static void main(String[] args) throws Exception {
		Configuration conf;
		Job job;
		// get input and output directories
		String input = (args.length > 0) ? args[0] : INPUT;
		String output = (args.length > 1) ? args[1] : OUTPUT;

		// create the first job to read the input file(s)
		int jobID = 1; // job id
		conf = new Configuration();
		conf.set("jobID", jobID + ""); // set job id
        job = Job.getInstance(conf, "Connected Components" + jobID);

		// set the class of each stage in mapreduce
		job.setJarByClass(ConnectedComponents.class);
		job.setMapperClass(TextInputMapper.class);
		job.setReducerClass(VertexReducer.class);

		// input and output paths
		Path in = new Path(input);
		Path out = new Path(output + "/job" + jobID);

		// delete the output path if it already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		// set the input and output paths
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// set the input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // binary format to use as input for the next job
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VertexWritable.class);

		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(JobCounter.JOB).getValue();
		// next job
		while (counter > 0) {
			// create a new job
			jobID++;
			conf = new Configuration();
			conf.set("jobID", jobID + "");
			job = Job.getInstance(conf, "Connected Components" + jobID);

			// set the class of each stage in mapreduce
			job.setJarByClass(ConnectedComponents.class);
			job.setMapperClass(VertexMapper.class);
			job.setReducerClass(VertexReducer.class);

			// input and output paths
			in = new Path(output + "/job" + (jobID - 1));
			out = new Path(output + "/job" + jobID);

			// delete the output path if it already exists
			fs = FileSystem.get(conf);
			if (fs.exists(out)) {
				fs.delete(out, true);
			}

			// set the input and output paths
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);

			// set the input and output formats
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class); // binary format to use as input for the next job
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(VertexWritable.class);

			job.waitForCompletion(true);
			counter = job.getCounters().findCounter(JobCounter.JOB).getValue();
		}

		// final job to write the number of connected components
		conf = new Configuration();
		job = Job.getInstance(conf, "Connected Components Final");

		// set the class of each stage in mapreduce
		job.setJarByClass(ConnectedComponents.class);
		job.setMapperClass(MinVertexIDMapper.class);
		job.setReducerClass(TextOutputReducer.class);

		// input and output paths
		in = new Path(output + "/job" + jobID);
		out = new Path(output + "/final");

		// delete the output path if it already exists
		fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		// set the input and output paths
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// set the input and output formats
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class); // actual number of connected components
		job.setOutputValueClass(NullWritable.class); // null because there is only one value as output

		job.waitForCompletion(true);
    }
}
