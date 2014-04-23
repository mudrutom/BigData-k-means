package cz.cvut.bigdata.kmeans.clusters;

import cz.cvut.bigdata.kmeans.vector.VectorUtils;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusteringReducer extends Reducer<ClusterKeyWritable, VectorWritable, IntWritable, Text> {

	private final IntWritable cluster = new IntWritable();
	private final Text text = new Text();

	@Override
	protected void reduce(ClusterKeyWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
		// compute a new centroid
		final VectorWritable centroid = VectorUtils.findMean(values);

		// emit the result
		cluster.set(key.getCluster());
		text.set(centroid.toString());
		context.write(cluster, text);
	}
}
