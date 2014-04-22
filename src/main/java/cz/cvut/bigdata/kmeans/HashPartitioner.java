package cz.cvut.bigdata.kmeans;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Simple partitioner based on the <i>hashCode()</i> method.
 */
public class HashPartitioner<K extends WritableComparable<? super K>> extends Partitioner<K, Object> {

	@Override
	public int getPartition(K key, Object value, int numPartitions) {
		return Math.abs(key.hashCode()) % numPartitions;
	}

}
