import java.util.Iterator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WeatherData {
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
		public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String line = text.toString(); // Transfer to String type
            String date = line.substring(6, 14); // Get date data
            float tempMax = Float.parseFloat(line.substring(39, 45).trim()); // Get maximum temperature and transfer to float type
            float tempMin = Float.parseFloat(line.substring(47, 53).trim()); // Get minimum temperature and transfer to float type
            if (tempMax > 40.0) { // Check
                context.write(new Text("Hot Day " + date),
                        new Text(String.valueOf(tempMax)));
            }
            if (tempMin < 10) {
                context.write(new Text("Cold Day " + date),
                        new Text(String.valueOf(tempMin))); //
            }
        }
    }

	public static class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text Key, Iterable<Text> iterator, Context context) throws IOException, InterruptedException {
			String temperature = iterator.iterator().next().toString();
			context.write(Key, new Text(temperature));
		}
    }

    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WeatherData");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

		job.setJarByClass(WeatherData.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
		
		// set the output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// if the output path already exists, delete it
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
		// submit the job and wait for its completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

