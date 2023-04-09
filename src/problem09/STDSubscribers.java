import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class STDSubscribers {

    public static class STDMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// get the necessary fields from the input record
            String[] fields = value.toString().split("\\|");
			String fromPhoneNum = fields[0];
			String callStartTimeStr = fields[2];
			String callEndTimeStr = fields[3];
            String stdFlag = fields[4];

			// check if the call is STD call
            if (stdFlag.equals("1")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date callStartTime = null;
                Date callEndTime = null;
                try { // parse the date strings
					callStartTime = sdf.parse(callStartTimeStr);
                    callEndTime = sdf.parse(callEndTimeStr);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
				
				// calculate the duration of the call in minutes
				long durationInMinutes = (callEndTime.getTime() - callStartTime.getTime()) / (60 * 1000);

				// emit the phone number and duration of the call
				context.write(new Text(fromPhoneNum), new LongWritable(durationInMinutes));
            }
        }
    }
    public static class STDReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalDuration = 0; // total duration of the calls made by the subscriber

			// calculate the total duration of the calls made by the subscriber
			for (LongWritable val : values) {
				totalDuration += val.get();
			}
			// check if total duration is greater than 60 minutes
			// if yes, emit the phone number
			if (totalDuration > 60) {
				context.write(key, NullWritable.get());
			}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "STDSubscribers");
        job.setJarByClass(STDSubscribers.class);
        job.setMapperClass(STDMapper.class);
        job.setReducerClass(STDReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}