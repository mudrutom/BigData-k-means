package cz.cvut.bigdata.kmeans;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.kmeans.clusters.ClusterKeyWritable;
import cz.cvut.bigdata.kmeans.clusters.ClusteringMapper;
import cz.cvut.bigdata.kmeans.clusters.ClusteringPartitioner;
import cz.cvut.bigdata.kmeans.clusters.ClusteringReducer;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Main entry-point of the k-means application used for executing
 * MapReduce jobs. The main task is to perform the k-means clustering
 * of the given TF-IDF index for Czech wikipedia articles.
 */
public class Main extends Configured implements Tool {

	public static void main(String[] arguments) throws Exception {
		System.exit(ToolRunner.run(new Main(), arguments));
	}

	private Configuration conf;
	private FileSystem hdfs;

	@Override
	public int run(String[] arguments) throws Exception {
		final ArgumentParser parser = new ArgumentParser("k-means");

		parser.addArgument("k", true, true, "specify the K");
		parser.addArgument("input", true, true, "specify input directory");
		parser.addArgument("output", true, true, "specify output directory");
		parser.parseAndCheck(arguments);

		final int k = parser.getInt("k");
		final Path inputDir = new Path(parser.getString("input"));
		final Path outputDir = new Path(parser.getString("output"));
		final Path cacheDir = outputDir.suffix("cache");

		conf = getConf();
		hdfs = FileSystem.get(conf);

		initDistributedCache(k, cacheDir);

		final Job clusteringJob = prepareClusteringJob(k, inputDir, outputDir);

		// TODO execute Clustering job iteratively

		return clusteringJob.waitForCompletion(true) ? 0 : 1;
	}

	/** Initialise the distributed file cache. */
	private void initDistributedCache(int k, Path cacheDir) throws IOException {
		// delete cache directory (if it exists)
		if (hdfs.exists(cacheDir)) {
			hdfs.delete(cacheDir, true);
		}

		for (int i = 0; i < k; i++) {
			DistributedCache.addCacheFile(cacheDir.suffix("part-r-0" + i).toUri(), conf);
		}
	}

	/** Create and setup the clustering job. */
	private Job prepareClusteringJob(int k, Path input, Path output) throws IOException {
		final Job job = new Job(conf, "Clustering");

		job.setNumReduceTasks(k);

		// set MarReduce classes
		job.setJarByClass(ClusteringMapper.class);
		job.setMapperClass(ClusteringMapper.class);
		job.setReducerClass(ClusteringReducer.class);
		job.setPartitionerClass(ClusteringPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(ClusterKeyWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// setup input and output
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}

}
