package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class VertexMapper extends Mapper<LongWritable, VertexWritable, LongWritable, VertexWritable> {
	@Override
	protected void map(LongWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
		// emit the real vertex
		context.write(key, value);
		
		// only emit the messages if the vertex is activated
		if (value.isActivated()) {
			for (LongWritable vertex : value.getPointsTo()) {
				context.write(vertex, value.makeMessage());
			}
		}
	}
}
