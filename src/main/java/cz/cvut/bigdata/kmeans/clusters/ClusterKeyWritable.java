package cz.cvut.bigdata.kmeans.clusters;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterKeyWritable implements WritableComparable<ClusterKeyWritable> {

	private int cluster = 0;
	private String term = null;

	public void set(int cluster, String term) {
		this.cluster = cluster;
		this.term = term;
	}

	public int getCluster() {
		return cluster;
	}

	public void setCluster(int cluster) {
		this.cluster = cluster;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cluster);
		out.writeUTF(term);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cluster = in.readInt();
		term = in.readUTF();
	}

	@Override
	public int compareTo(ClusterKeyWritable o) {
		if (o == null) return -1;
		final int val = (cluster < o.cluster) ? -1 : ((cluster == o.cluster) ? 0 : 1);
		return (val != 0) ? val : ((term == null) ? 1 : term.compareTo(o.term));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof ClusterKeyWritable)) return false;

		final ClusterKeyWritable that = (ClusterKeyWritable) o;
		return cluster == that.cluster && ((term != null) ? term.equals(that.term) : that.term == null);
	}

	@Override
	public int hashCode() {
		return 31 * cluster + (term != null ? term.hashCode() : 0);
	}

	@Override
	public String toString() {
		return String.format("%s[%d]", (term != null) ? term : "", cluster);
	}
}
