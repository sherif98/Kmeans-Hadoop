package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;


public class KMeanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger logger = Logger.getLogger(KMeanMapper.class);

    public void map(LongWritable key,
                    Text value, Context context) throws IOException, InterruptedException {

        logger.info("mapper is being called");
        double[] point = PointUtils.parsePoint(value.toString());


        double[] nearestCentroid = Driver.centroids.get(0);

        double bestDistance = Double.MAX_VALUE;
        for (double[] centroid : Driver.centroids) {
            double tempDistance = PointUtils.calculateDistance(centroid, point);
            if (tempDistance < bestDistance) {
                nearestCentroid = centroid;
                bestDistance = tempDistance;
            }
        }
        context.write(new Text(PointUtils.toString(nearestCentroid)), new Text(PointUtils.toString(point)));
    }

}