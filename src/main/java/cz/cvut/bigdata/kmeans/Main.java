package cz.cvut.bigdata.kmeans;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.kmeans.clusters.ClusterKeyWritable;
import cz.cvut.bigdata.kmeans.clusters.ClusterOutputReducer;
import cz.cvut.bigdata.kmeans.clusters.ClusteringMapper;
import cz.cvut.bigdata.kmeans.clusters.ClusteringPartitioner;
import cz.cvut.bigdata.kmeans.clusters.ClusteringReducer;
import cz.cvut.bigdata.kmeans.norm.NormalizeMapper;
import cz.cvut.bigdata.kmeans.norm.NormalizeReducer;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
		final String outputDir = parser.getString("output");

		conf = getConf();
		hdfs = FileSystem.get(conf);

		// input/output dirs
		final Path norm = new Path(outputDir, "norm");
		final Path cache = new Path(outputDir, "cache");
		final Path means = new Path(outputDir, "means");
		final Path clusters = new Path(outputDir, "clusters");

		// run the normalize job
		final Job normalizeJob = prepareNormalizeJob(k, inputDir, norm);
		if (!normalizeJob.waitForCompletion(true)) return 1;

		// run the first clustering job
		initDistributedCache(cache, norm);
		Job clusteringJob = prepareClusteringJob(k, norm, means);
		if (!clusteringJob.waitForCompletion(true)) return 1;

		// execute other clustering jobs iteratively
		for (int i = 0; i < k; i++) {
			updateDistributedCache(cache, means);
			clusteringJob = prepareClusteringJob(k, norm, means);
			if (!clusteringJob.waitForCompletion(true)) return 1;
		}

		// run the final clustering job to output the results
		updateDistributedCache(cache, means);
		final Job clusterOutputJob = prepareClusterOutputJob(k, norm, clusters);
		return clusterOutputJob.waitForCompletion(true) ? 0 : 1;
	}

	/** Initialise the distributed file cache. */
	private void initDistributedCache(Path cacheDir, Path normDir) throws IOException {
		// delete cache directory (if it exists)
		if (hdfs.exists(cacheDir)) {
			hdfs.delete(cacheDir, true);
		}
		hdfs.mkdirs(cacheDir);

		// list all the produced files
		for (FileStatus status : hdfs.listStatus(normDir)) {
			Path file = status.getPath();
			if (file.getName().startsWith("centroid")) {
				// move each centroid
				Path cacheFile = new Path(cacheDir, file.getName());
				hdfs.rename(file, cacheFile);
				DistributedCache.addCacheFile(cacheFile.toUri(), conf);
			}
		}
	}

	/** Update the distributed file cache. */
	private void updateDistributedCache(Path cacheDir, Path meansDir) throws IOException {
		// delete old cache directory
		hdfs.delete(cacheDir, true);
		hdfs.mkdirs(cacheDir);

		// list all the produced files
		for (FileStatus status : hdfs.listStatus(meansDir)) {
			// move each centroid
			Path file = status.getPath();
			Path cacheFile = new Path(cacheDir, file.getName().replace("part", "centroid"));
			hdfs.rename(file, cacheFile);
			DistributedCache.addCacheFile(cacheFile.toUri(), conf);
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
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}

	/** Create and setup the cluster output job. */
	private Job prepareClusterOutputJob(int k, Path input, Path output) throws IOException {
		final Job job = new Job(conf, "ClusterOutput");

		job.setNumReduceTasks(k);

		// set MarReduce classes
		job.setJarByClass(ClusteringMapper.class);
		job.setMapperClass(ClusteringMapper.class);
		job.setReducerClass(ClusterOutputReducer.class);
		job.setPartitionerClass(ClusteringPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(ClusterKeyWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// setup input and output
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "centroid", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "prototype", TextOutputFormat.class, IntWritable.class, Text.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}

	/** Create and setup the normalize job. */
	private Job prepareNormalizeJob(int k, Path input, Path output) throws IOException {
		final Job job = new Job(conf, "Normalize");

		job.setNumReduceTasks(k);

		// set MarReduce classes
		job.setJarByClass(NormalizeMapper.class);
		job.setMapperClass(NormalizeMapper.class);
		job.setReducerClass(NormalizeReducer.class);
		job.setPartitionerClass(HashPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// setup input and multiple outputs
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "centroid", TextOutputFormat.class, IntWritable.class, Text.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}

}
