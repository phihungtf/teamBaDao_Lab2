package mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TextOutputReducer extends Reducer<IntWritable, LongWritable, LongWritable, NullWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		// use a set to remove duplicates
        HashSet<LongWritable> set = new HashSet<LongWritable>();
		for (LongWritable value : values) {
			set.add(value);
		}

		context.write(new LongWritable(set.size()), NullWritable.get());
	}
}
