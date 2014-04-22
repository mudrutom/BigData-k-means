package cz.cvut.bigdata.kmeans;

import cz.cvut.bigdata.cli.ArgumentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

		conf = getConf();
		hdfs = FileSystem.get(conf);

		// create the k-means job
		final Job kMeansJob = prepareKMeansJob(k, inputDir, outputDir);

		// TODO execute k-means job iteratively

		// execute the job
		return kMeansJob.waitForCompletion(true) ? 0 : 1;
	}

	/** Create and setup the k-means job. */
	private Job prepareKMeansJob(int k, Path input, Path output) throws IOException {
		final Job job = new Job(conf, "k-means");

		job.setNumReduceTasks(k);

		// set MarReduce classes
		job.setJarByClass(KMeansMapper.class);
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setPartitionerClass(HashPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
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
