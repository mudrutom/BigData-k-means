package cz.cvut.bigdata.kmeans.clusters;

import cz.cvut.bigdata.kmeans.vector.VectorUtils;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClusteringMapper extends Mapper<Text, Text, ClusterKeyWritable, VectorWritable> {

	private final ClusterKeyWritable clusterKey = new ClusterKeyWritable();
	private final VectorWritable vector = new VectorWritable();

	private int k;
	private VectorWritable[] centroids;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		k = context.getNumReduceTasks();
		centroids = new VectorWritable[k];

		// load current centroids from the distributed cache
		final Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path cacheFile : cacheFiles) {
			FileSystem fileSystem = cacheFile.getFileSystem(context.getConfiguration());
			String content = IOUtils.toString(fileSystem.open(cacheFile));
			String[] parts = StringUtils.split(content, '\t');
			centroids[Integer.parseInt(parts[0])] = new VectorWritable().parse(parts[1]);
		}
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		vector.parse(value.toString());

		// find the nearest centroid (with cosine similarity)
		final double[] similarity = new double[k];
		for (int i = 0; i < k; i++) {
			similarity[i] = VectorUtils.cosineSimilarity(centroids[i], vector);
		}
		int cluster = 0;
		for (int i = 0; i < k; i++) {
			if (similarity[i] > similarity[cluster]) {
				cluster = i;
			}
		}

		// emit the result to appropriate reducer
		clusterKey.set(cluster, key.toString());
		context.write(clusterKey, vector);
	}

}
