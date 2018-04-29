/*
 * Driver.java
 *
 * Copyright 2014 Luca Menichetti <meniluca@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 *
 */

package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Driver extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(Driver.class);

    public static List<double[]> centroids = new ArrayList<double[]>();

    public static String INITIAL_CENTROID_FILE_NAME = "/home/sherif/Workspace/Java/wordcount-hadoop/maven-hadoop-java-wordcount-template/input/centroids.txt";

    public static String CENTROID_FILE_NAME = "/part-r-00000";

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), (Tool) new Driver(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    private static void readCentroids(String fileName) throws FileNotFoundException {
        centroids.clear();
        Scanner scanner = new Scanner(new File(fileName));
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] split = line.split(",");
            double[] point = new double[split.length];
            for (int i = 0; i < split.length; i++) {
                point[i] = Double.parseDouble(split[i]);
            }
            centroids.add(point);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            return 1;
        }
        int iterations = Integer.parseInt(args[2]);


        for (int i = 0; i < iterations; i++) {
            Path outputPath = new Path(args[1] + i);

            if (i == 0) {
                readCentroids(INITIAL_CENTROID_FILE_NAME);
            } else {
                readCentroids(args[1] + (i - 1) + CENTROID_FILE_NAME);
            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Your job name");
            job.setJarByClass(Driver.class);

            job.setMapperClass(KMeanMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(KMeanReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            Path filePath = new Path(args[0]);
            FileInputFormat.setInputPaths(job, filePath);

            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
        }
        return 0;
    }
}
