package cz.cvut.bigdata.kmeans.vector;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

public class VectorUtils {

	public static final double MIN_VALUE_THRESHOLD = 1.0 / 100000.0;

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

		final Set<Long> indices = (one.size() <= two.size()) ? one.indices() : two.indices();
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

	public static void addToMean(VectorWritable mean, VectorWritable vector) {
		for (Long index : vector.indices()) {
			double value = mean.get(index);
			mean.set(index, value + vector.get(index));
		}
	}

	public static void normalizeMean(VectorWritable mean, int vectorCount) {
		final LinkedList<Long> zeros = new LinkedList<Long>();
		for (Long index : mean.indices()) {
			double value = mean.get(index) / vectorCount;
			if (value < MIN_VALUE_THRESHOLD) {
				zeros.add(index);
			} else {
				mean.set(index, value);
			}
		}
		for (Long index : zeros) {
			mean.setZero(index);
		}
	}

}
