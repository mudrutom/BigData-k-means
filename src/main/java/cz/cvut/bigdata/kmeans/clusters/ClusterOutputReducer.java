package cz.cvut.bigdata.kmeans.clusters;

import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.FileReader;
import java.io.IOException;

public class ClusterOutputReducer extends Reducer<ClusterKeyWritable, VectorWritable, IntWritable, Text> {

	private final IntWritable cluster = new IntWritable();
	private final Text term = new Text();

	private MultipleOutputs outputs;
	private VectorWritable mean;

	@Override
	public void setup(Context context) {
		outputs = new MultipleOutputs(context);
	}

	@Override
	protected void reduce(ClusterKeyWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
		if (mean == null) {
			// load the mean from the distributed cache
			final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path cacheFile : cacheFiles) {
				if (cacheFile.getName().startsWith("centroid")) {
					String content = IOUtils.toString(new FileReader(cacheFile.toString()));
					String[] parts = StringUtils.split(content, '\t');
					if (Integer.parseInt(parts[0]) == key.getCluster()) {
						mean = new VectorWritable().parse(parts[1]);

						// write the centroid
						cluster.set(key.getCluster());
						outputs.write("centroid", cluster, new Text(mean.toString()));
						break;
					}
				}
			}
		}

		// emit the term with its cluster
		cluster.set(key.getCluster());
		term.set(key.getTerm());
		context.write(cluster, term);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputs.close();
	}
}
