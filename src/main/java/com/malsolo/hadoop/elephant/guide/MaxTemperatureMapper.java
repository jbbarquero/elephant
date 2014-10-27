/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malsolo.hadoop.elephant.guide;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jbeneito
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING_POSITIVE = 9999;
    private static final int MISSING_NEGATIVE = -9999;
//    private static final String QUALITY = "[01459]";
    private static final String STATION = "STATION";
    private static final String DOTS = "-------";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String firstWord = line.substring(0, 7);
//        System.out.printf("First word %s\n", firstWord);
        switch (firstWord) {
            case STATION:
//                System.out.println("Skip first line");
                break;
            case DOTS:
//                System.out.println("Skip second line");
                break;
            default:
                String date = line.substring(102, 111);
//                String temperature = line.substring(253, 258);
//                System.out.printf("Processing %s with %s\n", date, temperature);
                int airTemperature;
                if (line.charAt(253) == '+') {
                    airTemperature = Integer.parseInt(line.substring(254, 258).trim());
                } else {
                    airTemperature = Integer.parseInt(line.substring(253, 258).trim());
                }
//              String quality = line.substring(280, 281);
                if (airTemperature != MISSING_POSITIVE && airTemperature != MISSING_NEGATIVE /*&& quality.matches(QUALITY)*/) {
                    context.write(new Text(date), new IntWritable(airTemperature));
                }
                break;
        }
    }

}
