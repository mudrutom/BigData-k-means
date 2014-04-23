package cz.cvut.bigdata.kmeans.clusters;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner based on the <i>ClusterKeyWritable</i> class.
 */
public class ClusteringPartitioner extends Partitioner<ClusterKeyWritable, Object> {

	@Override
	public int getPartition(ClusterKeyWritable key, Object value, int numPartitions) {
		return key.getCluster();
	}

}
