import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageSalary {
  public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
    private Text id = new Text();
    private FloatWritable salary = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line, ",");
      while (itr.hasMoreTokens()) {
        id.set(itr.nextToken());
        salary = new FloatWritable(Float.parseFloat(itr.nextToken()));
        context.write(id, salary);
      }
    }
  }

  public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0;
      float count = 0;
      for (FloatWritable val : values) {
        sum += val.get();
        count++;
      }
      result.set(sum / count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Salary");
    job.setJarByClass(AverageSalary.class);
    job.setMapperClass(avgMapper.class);
    job.setCombinerClass(avgReducer.class);
    job.setReducerClass(avgReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    Path p = new Path(args[0]);
    Path p1 = new Path(args[1]);
    FileInputFormat.addInputPath(job, p);
    FileOutputFormat.setOutputPath(job, p1);
    job.waitForCompletion(true);
  }
}