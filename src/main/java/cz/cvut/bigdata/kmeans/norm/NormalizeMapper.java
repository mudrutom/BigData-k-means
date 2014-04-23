package cz.cvut.bigdata.kmeans.norm;

import cz.cvut.bigdata.kmeans.vector.VectorUtils;
import cz.cvut.bigdata.kmeans.vector.VectorWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NormalizeMapper extends Mapper<Text, Text, Text, VectorWritable> {

	private final VectorWritable vector = new VectorWritable();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// parse and normalize the vector
		vector.parse(value.toString());
		VectorUtils.normalize(vector);

		// emmit the result
		context.write(key, vector);
	}
}
