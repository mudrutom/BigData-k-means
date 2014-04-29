package cz.cvut.bigdata.kmeans.clusters;

import cz.cvut.bigdata.kmeans.vector.VectorUtils;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusteringReducer extends Reducer<ClusterKeyWritable, VectorWritable, IntWritable, Text> {

	private VectorWritable mean;
	private int cluster, vectorCount;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mean = null;
		cluster = 0;
		vectorCount = 0;
	}

	@Override
	protected void reduce(ClusterKeyWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
		if (mean == null) {
			// load the mean from the distributed cache
			final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path cacheFile : cacheFiles) {
				FileSystem fileSystem = cacheFile.getFileSystem(context.getConfiguration());
				String content = IOUtils.toString(fileSystem.open(cacheFile));
				String[] parts = StringUtils.split(content, '\t');
				if (Integer.parseInt(parts[0]) == key.getCluster()) {
					mean = new VectorWritable().parse(parts[1]);
					cluster = key.getCluster();
					break;
				}
			}
		}

		// recompute the mean
		VectorUtils.addToMean(mean, values.iterator().next());
		vectorCount++;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// normalize and write the resulting mean
		VectorUtils.normalizeMean(mean, vectorCount);
		context.write(new IntWritable(cluster), new Text(mean.toString()));
	}
}
