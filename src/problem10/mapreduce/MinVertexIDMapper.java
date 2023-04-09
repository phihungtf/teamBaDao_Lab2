package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MinVertexIDMapper extends Mapper<LongWritable, VertexWritable, IntWritable, LongWritable> {
	@Override
	protected void map(LongWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
		// emit all the min vertex ids
		context.write(new IntWritable(1), value.getMinVertexId());
	}
}
