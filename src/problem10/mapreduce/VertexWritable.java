package mapreduce;

import java.io.*;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable, Cloneable {
	private LongWritable minVertexId; // the minimum vertex id
	private TreeSet<LongWritable> pointsTo; // the vertices that this vertex points to
	private boolean activated; // whether this vertex is activated

	public VertexWritable() {
		super();
		this.minVertexId = null;
		this.pointsTo = new TreeSet<>();
		this.activated = false;
	}

	public VertexWritable(LongWritable minVertexId) {
		super();
		this.minVertexId = minVertexId;
		this.pointsTo = null;
		this.activated = false;
	}

	public boolean isMessage() {
		return pointsTo == null;
	}

	public VertexWritable makeMessage() {
		return new VertexWritable(this.minVertexId);
	}

	public void addVertex(LongWritable id) {
		if (pointsTo == null) pointsTo = new TreeSet<>();
		pointsTo.add(id);
	}

	// override write method in order to write to File
	@Override
	public void write(DataOutput out) throws IOException {
		this.minVertexId.write(out);
		if (this.pointsTo == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(this.pointsTo.size());
			for (LongWritable id : this.pointsTo) {
				id.write(out);
			}
		}
		out.writeBoolean(this.activated);
	}

	// override readFields method in order to read from File
	@Override
	public void readFields(DataInput in) throws IOException {
		this.minVertexId = new LongWritable();
		this.minVertexId.readFields(in);
		int size = in.readInt();
		if (size > -1) {
			pointsTo = new TreeSet<LongWritable>();
			for (int i = 0; i < size; i++) {
				LongWritable id = new LongWritable();
				id.readFields(in);
				this.pointsTo.add(id);
			}
		} else {
			this.pointsTo = null;
		}
		this.activated = in.readBoolean();
	}

	// override clone method in order to clone VertexWritable
	@Override
	public VertexWritable clone() {
		VertexWritable clone = new VertexWritable(this.minVertexId);
		if (this.pointsTo != null) {
			clone.pointsTo = new TreeSet<>();
			for (LongWritable id : this.pointsTo) {
				clone.pointsTo.add(id);
			}
		}
		// clone.activated = this.activated;
		return clone;
	}

	@Override
	public String toString() {
		if (this.pointsTo == null) return minVertexId + "";
		return minVertexId + " > " + pointsTo.toString();
	}

	// getter and setter
	public LongWritable getMinVertexId() {
		return minVertexId;
	}

	public void setMinVertexId(LongWritable id) {
		// set minVertexId to id if id is smaller than minVertexId
		if (this.minVertexId == null) {
			this.minVertexId = id;
			return;
		}
		if (id.get() < this.minVertexId.get()) this.minVertexId = id;
	}

	public TreeSet<LongWritable> getPointsTo() {
		return pointsTo;
	}

	public void setPointsTo(TreeSet<LongWritable> pointsTo) {
		this.pointsTo = pointsTo;
	}

	public boolean isActivated() {
		return activated;
	}

	public void setActivated(boolean activated) {
		this.activated = activated;
	}
}
