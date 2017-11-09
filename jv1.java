package src.bin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class jv1 {


  public static class Minmapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text target = new Text();
    private Integer weight = new IntWritable();

    public void map(Object key, Text value, Context context
      ) throws IOException, InterruptedException {

      String[] fields= value.toString().split("\t");

      target.set(fields[1]);
      weight.set(Integer.parseInt(fields[2]));
      context.write(target,weight);

    }
  }

  public static class Minreducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int min = 1000000;

      for (IntWritable val : values) {

        if (val.get()<min) {

          min = val.get();

        }
      }

      result.set(min);
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "jv1");

    job.setJarByClass(jv1.class);
    job.setMapperClass(Minmapper.class);
    job.setCombinerClass(Minreducer.class);
    job.setReducerClass(Minreducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
