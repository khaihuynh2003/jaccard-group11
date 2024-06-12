package org.jaccard;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer1 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniqueDocs = new HashSet<>();
        Set<String> allDocs = new TreeSet<>();

        for (Text val : values) {
            uniqueDocs.add(val.toString());
            allDocs.add(val.toString());
        }

        // Only output terms that appear in more than one document
        if (uniqueDocs.size() != context.getConfiguration().getInt("totalDocs", 0) && uniqueDocs.size() != 1) {
            StringBuilder sb = new StringBuilder();
            for (String doc : allDocs) {
                sb.append(doc).append(" ");
            }
            context.write(key, new Text(sb.toString().trim()));
        }
    }
}
