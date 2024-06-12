package org.jaccard;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text pair = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] termAndElements = line.split("\t");
        String term = termAndElements[0];
        String[] elements = termAndElements[1].split(" ");

        for (int i = 0; i < elements.length; i++) {
            String q = elements[i];
            String urlq = q.split("@")[0];
            String wq = q.split("@")[1];

            for (int j = 0; j < elements.length; j++) {
                if (i != j) {
                    String element = elements[j];
                    String url = element.split("@")[0];
                    String w = element.split("@")[1];

                    pair.set(url + " " + urlq + " " + w + " " + wq);
                    context.write(pair, one);
                }
            }
        }
    }
}
