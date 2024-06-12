package org.jaccard;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text term = new Text();
    private Text urlAndLength = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        String url = fileName.substring(0, fileName.indexOf('.')); // Extract document id from file name

        StringTokenizer itr = new StringTokenizer(value.toString());

        // Use a HashSet to store terms and remove duplicates
        HashSet<String> uniqueTerms = new HashSet<>();

        while (itr.hasMoreTokens()) {
            uniqueTerms.add(itr.nextToken());
        }

        int length = uniqueTerms.size(); // Length is now the number of unique terms

        // Emit each unique term
        for (String uniqueTerm : uniqueTerms) {
            term.set(uniqueTerm);
            urlAndLength.set(url + "@" + length);
            context.write(term, urlAndLength);
        }
    }
}
