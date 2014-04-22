package cz.cvut.bigdata.kmeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KMeansMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO load current centroids from the distributed cache
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// TODO find the nearest centroid
	}

}
