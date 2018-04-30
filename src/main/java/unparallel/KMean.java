package unparallel;

import com.example.PointUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class KMean {

    private static List<Point> readPoints(String filename) throws IOException {
        return Files.lines(Paths.get(filename))
                .map(PointUtils::parsePoint)
                .map(Point::new)
                .collect(Collectors.toList());
    }

    private static Optional<Point> findNearset(List<Point> centroids, Point point) {
        return centroids.stream()
                .min(Comparator.comparingDouble(c -> PointUtils.calculateDistance(c.getDimensions(), point.getDimensions())));
    }

    private static double[] sum(double[] a, double[] b) {
        double[] c = new double[a.length];
        Arrays.setAll(c, i -> a[i] + b[i]);
        return c;
    }

    private static Optional<Point> calculateNewCentroid(List<Point> cluster) {
        int size = cluster.size();
        return cluster.stream()
                .map(Point::getDimensions)
                .reduce(KMean::sum)
                .flatMap(p -> Optional.of(Arrays.stream(p).map(d -> d / size).toArray()))
                .map(Point::new);
    }

    private static List<Point> step(List<Point> datapoints, List<Point> prevCentroids) {
        return datapoints.stream()
                .collect(Collectors.groupingBy(p -> findNearset(prevCentroids, p)))
                .values()
                .stream()
                .map(KMean::calculateNewCentroid)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        List<Point> datapoints = readPoints("points/data.txt");
        List<Point> centroids = readPoints("input/centroids.txt");
        for (int i = 0; i < Integer.parseInt(args[0]); ++i) {
            centroids = step(datapoints, centroids);
        }
        centroids.forEach(p -> System.out.println(Arrays.toString(p.getDimensions())));
        long elapsedTime = System.currentTimeMillis() - start;
        System.out.println(elapsedTime + "ms");
    }
}