import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.Logger;
// import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
// import java.util.Collections;


public class BookQuery {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
        private final Set<String> queryTermSet = new HashSet<>();

        // setup is called ONCE for each mapper, then all calls to the map func is made
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read the query terms from the configuration and store them in a set
            String query = context.getConfiguration().get("query");
            // Split the query into words
            String[] queryWords = query.split("\\W+"); // String[] queryTermsArray = query.split("[, ?.\n]+");
            // Create terms as 3 consecutive words
            for (int i = 0; i < queryWords.length - 2; i++) {
                String term = queryWords[i] + " " + queryWords[i + 1] + " " + queryWords[i + 2];
                queryTermSet.add(term);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the current document name to use as URL
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileNameAsUrl = fileSplit.getPath().getName().split("\\.")[0];

            // Split the document into words
            String[] words = value.toString().split("\\W+");
            // Create terms as 3 consecutive words
            Set<String> mapTermSet = new HashSet<>();
            for (int i = 0; i < (words.length - 2); i++) {
                String term = words[i] + " " + words[i + 1] + " " + words[i + 2];
                mapTermSet.add(term);
            }
            // Filter out terms that are not in the query
            mapTermSet.retainAll(queryTermSet);

            // Emit intermediate key-value pairs [term, URL@length]
            // int length = mapTermSet.size();
            for (String term : mapTermSet) {
                System.out.println(fileNameAsUrl + "   " + term);
                context.write(new Text(fileNameAsUrl), new Text(term));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text Url, Iterable<Text> terms, Context context) throws IOException, InterruptedException {
            // Get number of terms in the current document
            // Iterable can only be traversed once
            Set<String> TermSet = new HashSet<>();
            for (Text term : terms) {
                TermSet.add(term.toString());
            }

            int termCount = 0;
            for (String term : TermSet) {
                termCount++;
            }

            for (String term : TermSet) {
                context.write(new Text(term), new Text(Url + "@" + termCount));
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String term = value.toString().split("\t")[0];
            String UrlAndLength = value.toString().split("\t")[1];

            context.write(new Text(term), new Text(UrlAndLength));

        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
        // auto-increment LongWritable key
        // private long autoIncrement = 0;

        @Override
        protected void reduce(Text termAsKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // autoIncrement += 1;

            // Read input
            List<String> urlAndLengthList = new ArrayList<>();
            for (Text value : values) {
                urlAndLengthList.add(value.toString());
            }

            // Get the number of total documents (this should be set somewhere in the context configuration)
            int n = context.getConfiguration().getInt("totalDocuments", 0);

            // Check if the length of the group is neither n nor 1
            if (urlAndLengthList.size() != n && urlAndLengthList.size() != 1) {
                // Sort the group
                urlAndLengthList.sort((a, b) -> Integer.compare(Integer.parseInt(b.split("@")[1]), Integer.parseInt(a.split("@")[1])));

                // Emit the term and sorted group
                // context.write(new Text(termAsKey), new Text(String.join(",", urlAndLengthList)));
                context.write(new Text(termAsKey), new Text(String.join(",", urlAndLengthList)));
            }
        }
    }

    public static class Mapper3 extends Mapper<Object, Text, Text, Text> {
        /* private final Set<String> queryTermSet = new HashSet<>();
        private String queryTermsCount;

        // setup is called ONCE for each mapper, then all calls to the map func is made
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read the query terms from the configuration and store them in a set
            String query = context.getConfiguration().get("query");
            // Split the query into words
            String[] queryWords = query.split("\\W+");
            // Create terms as 3 consecutive words
            for (int i = 0; i < (queryWords.length - 2); i++) {
                String term = queryWords[i] + " " + queryWords[i + 1] + " " + queryWords[i + 2];
                queryTermSet.add(term);
            }
            queryTermsCount = Integer.toString(queryTermSet.size()); // String queryTermsCount = String.valueOf(queryTerms.size());
        } */
        @Override
        protected void map(Object autoIncrement, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input value to get the list of URLs
            ArrayList<String> urlAndLengthArray = new ArrayList<>(Arrays.asList(value.toString().split("\t")[1].split(",")));
            // String[] urlAndLengthArray = value.toString().split("\t")[1].split(",");

            // Find the query URL and length
            String urlAndLengthQuery = "";
            for (String urlAndLength : urlAndLengthArray) {
                if (urlAndLength.split("@")[0].equals("query")) {
                    urlAndLengthQuery = urlAndLength;
                    break;
                }
            }
            urlAndLengthArray.remove(urlAndLengthQuery);
            // Don't know how query can be missing from urlAndLengthArray, but a small portion of the key-value pair {term, [url@w]} does not have query in [url@w]. It maybe has something to do with Speculative Execution.
            if (!urlAndLengthQuery.isEmpty()) {
                // Emit pairs of URLs for Jaccard similarity calculation
                String oneString = "1";
                for (String urlAndLength : urlAndLengthArray) {
                    context.write(new Text(urlAndLength + "@" + urlAndLengthQuery), new Text(oneString));
                }
            }
        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
                // basically sum += 1;
            }

            // Parse the URLs and lengths from the key
            String[] urlOrLength = key.toString().split("@");
            int len1 = Integer.parseInt(urlOrLength[1]);
            int len2 = Integer.parseInt(urlOrLength[3]);

            // Calculate the Jaccard similarity
            double jaccard = (sum != (len1 + len2)) ? ((double) sum / ((len1 + len2) - sum)) : 1;
            context.write(new Text(urlOrLength[0] + " - " + urlOrLength[2]), new Text(String.valueOf(jaccard)));
        }
    }

    public static void main(String[] args) throws Exception {
        // System.out.println(System.getProperty("java.version"));
        /* Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <input> <intermediate output> <final output>");
            System.exit(2);
        } */
        // BasicConfigurator.configure();

        if (args.length != 3) {
            System.err.println("Usage: <input> <intermediate output> <final output>");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        String queryPath = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(input), conf);

        // Read the query file from HDFS
        Path queryFilePath = new Path(queryPath);
        FSDataInputStream inputStream = fs.open(queryFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder query = new StringBuilder();
        String line = reader.readLine();
        while (line != null){
            query.append(line).append(" ");
            line = reader.readLine();
        }
        // Add query to the configuration
        conf.set("query", query.toString());
        // closing resources
        reader.close();
        inputStream.close();

        // Add the file count to the configuration
        Path inputPath = new Path(input);
        ContentSummary cs = fs.getContentSummary(inputPath);
        long fileCount = cs.getFileCount();
        conf.setLong("totalDocuments", fileCount);


        // Configure and run the first MapReduce job
        Job job1 = Job.getInstance(conf, "Jaccard Similarity - Job 1");
        job1.setJarByClass(BookQuery.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input));
        Path temp1 = new Path(output + "_temp1");
        FileOutputFormat.setOutputPath(job1, temp1);

        job1.waitForCompletion(true);


        // Configure and run the second MapReduce job
        Job job2 = Job.getInstance(conf, "Jaccard Similarity - Job 2");
        job2.setJarByClass(BookQuery.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, temp1);
        Path temp2 = new Path(output + "_temp2");
        FileOutputFormat.setOutputPath(job2, temp2);

        job2.waitForCompletion(true);


        // Configure and run the third MapReduce job
        Job job3 = Job.getInstance(conf, "Jaccard Similarity - Job 3");
        job3.setJarByClass(BookQuery.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, temp2);
        FileOutputFormat.setOutputPath(job3, new Path(output));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}