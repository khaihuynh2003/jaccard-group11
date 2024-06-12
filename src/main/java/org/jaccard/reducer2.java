package org.jaccard;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer2 extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable similarity = new DoubleWritable();
    private Set<String> seenPairs = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        String[] parts = key.toString().split(" ");
        int wi = Integer.parseInt(parts[2]);
        int wj = Integer.parseInt(parts[3]);
        double sim = (double) sum / (wi + wj - sum);

        similarity.set(sim);

        // Ensure consistent ordering
        String doc1 = parts[0];
        String doc2 = parts[1];
        String orderedPair = doc1.compareTo(doc2) < 0 ? doc1 + " " + doc2 : doc2 + " " + doc1;

        // Check if the pair has already been seen
        if (!seenPairs.contains(orderedPair) && sim >= 0.1 && sim < 1) {
            context.write(new Text(orderedPair), similarity);
            seenPairs.add(orderedPair);
        }

    }
}
