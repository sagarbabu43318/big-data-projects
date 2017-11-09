package src.bin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import java.util.StringTokenizer;
import java.io.IOException;

public class jv2 {

  public static class mapper1
    extends Mapper<Object, Text, Text, IntWritable>{

    private Text src = new Text();
    private Text tgt = new Text();
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable negone = new IntWritable(-1);

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

      StringTokenizer x = new StringTokenizer(value.toString());

        while (x.hasMoreTokens()){

          src.set(x.nextToken());
          context.write(src,one);
          tgt.set(x.nextToken());
          context.write(tgt,negone);
        }
      }
    }

  public static class reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int tot = 0;

      for (IntWritable val : values) {

          tot += val.get();

      }
      result.set(tot);
      context.write(key,result);
    }
  }



  public static class mapper2
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text diff = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
      ) throws IOException, InterruptedException {

      StringTokenizer y = new StringTokenizer(value.toString());

        while (y.hasMoreTokens()){

          y.nextToken();
          diff.set(y.nextToken());
          context.write(diff,one);
      }
    }
  }


  public static class reducer2
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable finalresult = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int finaltot = 0;

      for (IntWritable val : values) {

          finaltot = finaltot + val.get();

      }
      finalresult.set(finaltot);
      context.write(key,finalresult);
    }
  }


  public static void main(String[] args) throws Exception {

    String OUTPUT_PATH = "temp_output";

    Configuration conf = new Configuration();

    FileSystem hdfs = FileSystem.get(conf);

    Path output = new Path(OUTPUT_PATH);

    if (hdfs.exists(output)) {
    hdfs.delete(output, true);
    }

    Job job = Job.getInstance(conf, "job");

    job.setJarByClass(jv2.class);
    job.setMapperClass(mapper1.class);
    job.setCombinerClass(reducer1.class);
    job.setReducerClass(reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    job.waitForCompletion(true);

    Job job2 = new Job(conf, "job2");

    job2.setJarByClass(jv2.class);
    job2.setMapperClass(mapper2.class);
    job2.setCombinerClass(reducer2.class);
    job2.setReducerClass(reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
