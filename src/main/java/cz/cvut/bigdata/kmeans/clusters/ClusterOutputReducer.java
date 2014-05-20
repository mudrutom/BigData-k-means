package cz.cvut.bigdata.kmeans.clusters;

import cz.cvut.bigdata.kmeans.vector.VectorUtils;
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
import java.util.TreeSet;

public class ClusterOutputReducer extends Reducer<ClusterKeyWritable, VectorWritable, IntWritable, Text> {

	private final IntWritable cluster = new IntWritable();
	private final Text term = new Text();

	private MultipleOutputs outputs;
	private VectorWritable mean;

	private int k;
	private TreeSet<Prototype> prototypes;

	@Override
	public void setup(Context context) {
		outputs = new MultipleOutputs(context);
		k = context.getNumReduceTasks();
		prototypes = new TreeSet<Prototype>();
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

		// add the vector as a prototype
		final Prototype prototype = new Prototype(key.getTerm(), VectorUtils.cosineSimilarity(mean, values.iterator().next()));
		prototypes.add(prototype);
		if (prototypes.size() > k) {
			// remove the one with the lowest similarity
			prototypes.pollFirst();
		}

		// emit the term with its cluster
		term.set(key.getTerm());
		context.write(cluster, term);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// write the prototypes
		for (Prototype prototype : prototypes) {
			term.set(prototype.getTerm());
			outputs.write("prototype", cluster, term);
		}
		outputs.close();
	}

	public static class Prototype implements Comparable<Prototype> {

		private final String term;
		private final double similarity;

		public Prototype(String term, double similarity) {
			this.term = term;
			this.similarity = similarity;
		}

		public String getTerm() {
			return term;
		}

		public double getSimilarity() {
			return similarity;
		}

		@Override
		public int compareTo(Prototype o) {
			return Double.compare(similarity, o.similarity);
		}
	}
}
