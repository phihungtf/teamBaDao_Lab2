package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class VertexReducer extends Reducer<LongWritable, VertexWritable, LongWritable, VertexWritable> {
	// enum used for counter
	public static enum JobCounter {
		JOB
	}

	@Override
	protected void reduce(LongWritable key, Iterable<VertexWritable> values, Context context) throws IOException, InterruptedException {
        VertexWritable realVertex = null;
        LongWritable currentMinimalKey = null;
		
		for (VertexWritable vertex : values) {
			// clone if value is a real vertex
			if (!vertex.isMessage()) {
				if (realVertex == null) {
					realVertex = vertex.clone();
				}
			} else { // find the minimal key in the messages
				if (currentMinimalKey == null) {
					currentMinimalKey = new LongWritable(vertex.getMinVertexId().get());
				} else {
					if (currentMinimalKey.get() > vertex.getMinVertexId().get()) {
						currentMinimalKey = new LongWritable(vertex.getMinVertexId().get());
					}
				}
			}
		}

		if (currentMinimalKey != null && currentMinimalKey.get() < realVertex.getMinVertexId().get()) {
			realVertex.setMinVertexId(currentMinimalKey);
			realVertex.setActivated(true);
			// increment the counter cause we have a new min vertex id
			context.getCounter(JobCounter.JOB).increment(1);
		} else {
			realVertex.setActivated(false);
		}

		context.write(key, realVertex);
	}
}
