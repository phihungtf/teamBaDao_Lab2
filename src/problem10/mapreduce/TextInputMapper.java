package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextInputMapper extends Mapper<LongWritable, Text, LongWritable, VertexWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		VertexWritable vertex = new VertexWritable();
		LongWritable realKey = null;
		boolean isFirstNode = true;

		String[] values = value.toString().split("\\s+");
		for (String s : values) {
			if (isFirstNode) {
				// set the first node as the key
				realKey = new LongWritable(Long.parseLong(s));
				vertex.setMinVertexId(realKey);
				isFirstNode = false;
			} else {
				LongWritable id = new LongWritable(Long.parseLong(s));
				vertex.setMinVertexId(id);
				vertex.addVertex(id);
			}
		}

		context.write(realKey, vertex); // emit the real vertex

		// emit the messages
		for (LongWritable node : vertex.getPointsTo()) {
			context.write(node, vertex.makeMessage());
		}
	}
}
