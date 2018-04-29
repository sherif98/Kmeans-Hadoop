package com.example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class KMeanReducer extends Reducer<Text, Text, Text, NullWritable> {

    private static Logger logger = Logger.getLogger(KMeanReducer.class);

    @Override
    protected void reduce(Text centroid, Iterable<Text> pointsNearestToCentroid, Context context)
            throws IOException, InterruptedException {
        logger.info("reducer is being called");
        double[] newCenter = null;
        int numberOfElements = 0;
        for (Text aPointsNearestToCentroid : pointsNearestToCentroid) {

            double[] next = PointUtils.parsePoint(aPointsNearestToCentroid.toString());
            if (newCenter == null) {
                newCenter = new double[next.length];
            }
            for (int i = 0; i < next.length; i++) {
                newCenter[i] += next[i];
            }
            numberOfElements++;
        }

        if (newCenter != null) {
            for (int i = 0; i < newCenter.length; i++) {
                newCenter[i] /= numberOfElements;
            }
            logger.info(Arrays.toString(newCenter));
            context.write(new Text(PointUtils.toString(newCenter)), NullWritable.get());
        }
    }

}
