package cz.cvut.bigdata.kmeans.vector;

import java.util.LinkedHashSet;
import java.util.Set;

public class VectorUtils {

	private VectorUtils() { }

	public static void normalize(VectorWritable vector) {
		final Set<Long> indices = vector.indices();

		double sum = 0.0;
		for (Long index : indices) {
			sum += vector.get(index);
		}
		for (Long index : indices) {
			double value = vector.get(index);
			vector.set(index, value / sum);
		}
	}

	public static double cosineSimilarity(VectorWritable one, VectorWritable two) {
		double similarity = 0.0;

		final Set<Long> indices = new LinkedHashSet<Long>();
		indices.addAll(one.indices());
		indices.addAll(two.indices());

		for (Long index : indices) {
			similarity += one.get(index) * two.get(index);
		}

		return similarity;
	}

	public static double euclideanDistance(VectorWritable one, VectorWritable two) {
		double distance = 0.0;

		final Set<Long> indices = new LinkedHashSet<Long>();
		indices.addAll(one.indices());
		indices.addAll(two.indices());

		for (Long index : indices) {
			double delta = one.get(index) - two.get(index);
			distance += delta * delta;
		}
		distance = Math.sqrt(distance);

		return distance;
	}

	public static VectorWritable findMean(Iterable<VectorWritable> vectors) {
		final VectorWritable mean = new VectorWritable();

		int count = 0;
		for (VectorWritable vector : vectors) {
			for (Long index : vector.indices()) {
				double value = mean.get(index);
				mean.set(index, value + vector.get(index));
			}
			count++;
		}
		for (Long index : mean.indices()) {
			double value = mean.get(index);
			mean.set(index, value / count);
		}

		return mean;
	}

}
