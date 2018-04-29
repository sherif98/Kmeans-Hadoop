package com.example;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class KMeanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static Logger logger = Logger.getLogger(KMeanMapper.class);
//    @Override
//    public void configure(JobConf job) {
//
//        double[] p1 = {1, 2};
//        double[] p2 = {3, 4};
//        ArrayPrimitiveWritable arrayPrimitiveWritable = new ArrayPrimitiveWritable();
//        arrayPrimitiveWritable.set(p1);
//
//        ArrayPrimitiveWritable primitiveWritable = new ArrayPrimitiveWritable();
//        primitiveWritable.set(p2);
//
//        centroids.add(arrayPrimitiveWritable);
//        centroids.add(primitiveWritable);


//        try {
    // Fetch the file from Distributed Cache Read it and store the
    // centroid in the ArrayList
//            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
//            if (cacheFiles != null && cacheFiles.length > 0) {
//                String line;
//                centroids.clear();
//                BufferedReader cacheReader = new BufferedReader(
//                        new FileReader(cacheFiles[0].toString()));
//                try {
//                    // Read the file split by the splitter and store it in
//                    // the list
//                    while ((line = cacheReader.readLine()) != null) {
//                        centroids.add(PointUtils.parsePoint(line));
//                    }
//                } finally {
//                    cacheReader.close();
//                }
//            }
//        } catch (IOException e) {
//            System.err.println("Exception reading DistribtuedCache: " + e);
//        }
//    }


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
        context.write(new IntWritable(Arrays.hashCode(nearestCentroid)), new Text(PointUtils.toString(point)));
    }

}
