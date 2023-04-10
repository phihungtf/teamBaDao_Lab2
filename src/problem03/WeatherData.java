package com.hadoop.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WeatherData {
    public static class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter)
                throws IOException {
            String line = text.toString(); // Transfer to String type
            String date = line.substring(6, 14); // Get date data
            float tempMax = Float.parseFloat(line.substring(39, 45).trim()); // Get maximum temperature and transfer to float type
            float tempMin = Float.parseFloat(line.substring(47, 53).trim()); // Get minimum temperature and transfer to float type
            if (tempMax > 40.0) { // Check
                outputCollector.collect(new Text("Hot Day " + date),
                        new Text(String.valueOf(tempMax)));
            }
            if (tempMin < 10) {
                outputCollector.collect(new Text("Cold Day " + date),
                        new Text(String.valueOf(tempMin))); //
            }
        }

    }

    public static class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text Key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String temperature = iterator.next().toString();
            outputCollector.collect(Key, new Text(temperature));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WeatherData.class);
        conf.setJobName("temperature");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setMapperClass(MaxTemperatureMapper.class);
        conf.setReducerClass(MaxTemperatureReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}

