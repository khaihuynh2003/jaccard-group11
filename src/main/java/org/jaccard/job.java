package org.jaccard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class job {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CombinedJobDriver <input path> <output path> <totalDocs>");
            System.exit(-1);
        }

        Configuration conf1 = new Configuration();
        conf1.setInt("totalDocs", Integer.parseInt(args[2]));

        Job job1 = Job.getInstance(conf1, "Customized Inverted Index");
        job1.setJarByClass(job.class);

        job1.setMapperClass(mapper1.class);
        job1.setReducerClass(reducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tempOutputPath = new Path(args[1] + "_tmp");
        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Similarity Scores");
        job2.setJarByClass(job.class);

        job2.setMapperClass(mapper2.class);
        job2.setReducerClass(reducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, tempOutputPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
