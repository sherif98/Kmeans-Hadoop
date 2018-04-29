package com.example;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.util.Arrays;


public final class PointUtils {


    private PointUtils() {
    }

    public static double calculateDistance(double[] centroidAttributes, double[] pointAttributes) {
        double sum = 0.0;
        for (int i = 0; i < centroidAttributes.length; i++) {
            double thisCoordinate = centroidAttributes[i];
            double otherCoordinate = pointAttributes[i];
            sum += Math.pow(thisCoordinate - otherCoordinate, 2);
        }
        return Math.sqrt(sum);
    }


    public static double[] parsePoint(String line) {
        String[] attrs = line.split(",");
        double[] temp = new double[attrs.length];
        for (int i = 0; i < attrs.length; i++) {
            temp[i] = Double.parseDouble(attrs[i]);
        }
        return temp;
    }

    public static String toString(double[] arr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arr.length - 1; i++) {
            sb.append(arr[i]).append(",");
        }
        sb.append(arr[arr.length - 1]);
        return sb.toString();
    }

}
