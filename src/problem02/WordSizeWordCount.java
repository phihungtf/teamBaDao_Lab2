package com.hadoop.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordSizeWordCount {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable count;
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString(); // Transfer to String type
            StringTokenizer stringTokenizer = new StringTokenizer(line); // Split the input String into separate tokens
            while (stringTokenizer.hasMoreTokens()) {
                String eWord = stringTokenizer.nextToken();  // Assign eWord variable for each word
                count = new IntWritable(eWord.length());  // Length of each word
                word.set(eWord);
                context.write(count, word);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text x : values) {
                sum++; // Calculate frequency of each length of word
            }
            context.write(key, new IntWritable(sum));
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordSize");
        job.setJarByClass(WordSizeWordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

