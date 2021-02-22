package com.steps;

import com.data.AppConfig;
import com.data.MyKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sort {
    public static class MapperClass extends Mapper<LongWritable, Text, MyKey, Text> {
    String delim = AppConfig.delim;
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //input - danny went home \t 0.5
        String[] splitted= value.toString().split(delim);
        String[] ws= splitted[0].split(" ");
        Double p=Double.valueOf(splitted[1]);
        MyKey reducerKey= new MyKey(new Text(ws[0]+" "+ws[1]),new DoubleWritable(p));
        Text reducerVal= new Text(ws[2]);
        context.write(reducerKey,reducerVal);
    }

}


public static class ReducerClass extends Reducer<MyKey, Text, Text, Text> {
    @Override
    public void reduce(MyKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        //input example (MyKey(dany went, 0.5), home)
        for(Text value : values) {
            Text newKey = new Text(key.getText().toString() + " " + value.toString());
            context.write(newKey, new Text(key.prob.toString()));
        }
    }
}

    public static class PartitionerClass extends Partitioner<MyKey,Text> {

        @Override
        public int getPartition(MyKey key, Text value, int numPartitions) {
            return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }





    public static int run() throws IOException, ClassNotFoundException, InterruptedException {

        System.err.println("running sort");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort");

        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.MapperClass.class);
        job.setReducerClass(Sort.ReducerClass.class);
        job.setPartitionerClass(Sort.PartitionerClass.class);

        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(AppConfig.task4InputFile));
        FileOutputFormat.setOutputPath(job, new Path(AppConfig.task4OutputFile));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        int exit =job.waitForCompletion(true) ? 0 : 1;
        return exit;
    }

/**
 * for local run
 */
//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        run();
//    }
}
