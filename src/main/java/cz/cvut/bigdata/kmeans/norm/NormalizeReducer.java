package cz.cvut.bigdata.kmeans.norm;

import cz.cvut.bigdata.kmeans.HashPartitioner;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class NormalizeReducer extends Reducer<Text, VectorWritable, Text, Text> {

	private final Text text = new Text();

	private MultipleOutputs outputs;
	private boolean writeCentroid;

	@Override
	public void setup(Context context) {
		outputs = new MultipleOutputs(context);
		writeCentroid = true;
	}

	@Override
	protected void reduce(Text key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
		// write the vector
		text.set(values.iterator().next().toString());
		context.write(key, text);

		if (writeCentroid) {
			// write the centroid
			final HashPartitioner<Text> partitioner = new HashPartitioner<Text>();
			final IntWritable cluster = new IntWritable(partitioner.getPartition(key, text, context.getNumReduceTasks()));
			outputs.write("centroid", cluster, text);
			writeCentroid = false;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputs.close();
	}
}
