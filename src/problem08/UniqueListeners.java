import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueListeners {
    private enum COUNTERS {
        INVALID_RECORD_COUNT
    }

	public class LastFMConstants {
		public static final int USER_ID = 0;
		public static final int TRACK_ID = 1;
		public static final int IS_SHARED = 2;
		public static final int RADIO = 3;
		public static final int IS_SKIPPED = 4;
	}

    public static class UniqueListenersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        IntWritable trackId = new IntWritable();
        IntWritable userId = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]"); // Split input value with regex(|)
            trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID])); // Assign trackId
            userId.set(Integer.parseInt(parts[LastFMConstants.USER_ID])); // Assign userId
            if (parts.length == 5) { // Check invalid record
                context.write(trackId, userId);
            } else {
                context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }
        }
    }

    public static class UniqueListenersReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
        public void reduce(IntWritable trackId, Iterable<IntWritable> userIds, Context context)
                throws IOException, InterruptedException {
            Set<Integer> userIdSet = new HashSet<Integer>();
            for (IntWritable userId : userIds) { // Extracts the unique user IDs for a given track ID
                userIdSet.add(userId.get());
            }
            IntWritable size = new IntWritable(userIdSet.size());
            context.write(trackId, size); // Return key-value pair with trackId as a key and the number of user Id as a value
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: uniqueListeners <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Unique listeners per track");
        job.setJarByClass(UniqueListeners.class);
        job.setMapperClass(UniqueListenersMapper.class);
        job.setReducerClass(UniqueListenersReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
		// if the output path already exists, delete it
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        Counters counters = job.getCounters();
        System.out.println("No. of Invalid Records :"
                + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
                .getValue());
    }
}