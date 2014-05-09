package cz.cvut.bigdata.kmeans.vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class VectorWritable implements WritableComparable<VectorWritable> {

	private final Map<Long, Double> values = new LinkedHashMap<Long, Double>();

	public Double get(Long index) {
		return values.containsKey(index) ? values.get(index) : 0.0;
	}

	public void set(Long index, Double value) {
		values.put(index, value);
	}

	public void setZero(Long index) {
		if (values.containsKey(index)) {
			values.remove(index);
		}
	}

	public Set<Long> indices() {
		return values.keySet();
	}

	public int size() {
		return values.size();
	}

	public VectorWritable parse(String text) {
		final String[] parts = StringUtils.split(text);
		if (parts == null) throw new IllegalArgumentException();
		values.clear();
		for (String part : parts) {
			String[] val = StringUtils.split(part, ':');
			if (val.length < 2) throw new IllegalArgumentException();
			values.put(Long.parseLong(val[0]), Double.parseDouble(val[1]));
		}
		return this;
	}

	public void copy(VectorWritable vector) {
		values.clear();
		for (Map.Entry<Long, Double> value : vector.values.entrySet()) {
			values.put(value.getKey(), value.getValue());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.size());
		for (Map.Entry<Long, Double> value : values.entrySet()) {
			out.writeLong(value.getKey());
			out.writeDouble(value.getValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		values.clear();
		final int size = in.readInt();
		for (int i = 0; i < size; i++) {
			values.put(in.readLong(), in.readDouble());
		}
	}

	@Override
	public int compareTo(VectorWritable o) {
		if (o == null) return -1;
		return (size() < o.size()) ? -1 : ((size() == o.size()) ? 0 : 1);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof VectorWritable)) return false;

		final VectorWritable that = (VectorWritable) o;
		return values.equals(that.values);
	}

	@Override
	public int hashCode() {
		return values.hashCode();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for (Map.Entry<Long, Double> value : values.entrySet()) {
			sb.append(value.getKey()).append(':').append(value.getValue()).append(' ');
		}
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}
}
