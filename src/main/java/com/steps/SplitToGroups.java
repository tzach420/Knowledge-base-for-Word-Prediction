package com.steps;

import com.data.AppConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * this class get as input a 3-gram corpus, split the corpus to 2 groups and outputs a file that
 * each line contains (3-gram, (#of appearces in first group,#of appearces in second group))
 */

public class SplitToGroups {
    public static enum NGRAMS{
        VALUE

    };


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        String delim = AppConfig.delim;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //value = id    ngram   data
            String[] data = value.toString().split(delim);
            String ngram = data[0];
            String [] splitted = ngram.split(" ");
            if(splitted.length==3) {
                Text text = new Text(ngram);
                Random rand = new Random();
                IntWritable groupId = new IntWritable(rand.nextInt(2));
                String toWrite = data[2] + delim + "0";
                if (groupId.get() != 0) toWrite = "0" + delim + data[2];
                context.write(text, new Text(toWrite));
                context.getCounter(NGRAMS.VALUE).increment(Integer.valueOf(data[2]));
            }

        }



    }
    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {
        String delim =AppConfig.delim;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int group1Sum=0;
            int group2Sum=0;
            for(Text pair : values){
                String [] splitted = pair.toString().split(delim);
                group1Sum += Integer.valueOf(splitted[0]);
                group2Sum += Integer.valueOf(splitted[1]);
            }
            String toWrite = group1Sum +delim+ group2Sum;
            context.write(key,new Text(toWrite));
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        String delim =AppConfig.delim;

        /**
         *
         * @param key - 3gram
         * @param values - ["0 \t 1", "1 \t 0", "0 \t 1" ....]
         * @param context
         * @throws IOException
         * @throws InterruptedException
         * output - (3gram,"#in groupA \t #in groupB")
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int group1Sum=0;
            int group2Sum=0;
            for(Text pair : values){
                String [] splitted = pair.toString().split(delim);
                group1Sum += Integer.valueOf(splitted[0]);
                group2Sum += Integer.valueOf(splitted[1]);
            }
            String toWrite = group1Sum +delim+ group2Sum;
            context.write(key,new Text(toWrite));
      }
    }



    public static long run() throws IOException, ClassNotFoundException, InterruptedException {
        System.err.println("running splitToGroups");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "splitToGroups");

        job.setJarByClass(SplitToGroups.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(SplitToGroups.CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(AppConfig.task1InputFile));
        FileOutputFormat.setOutputPath(job, new Path(AppConfig.task1OutputFile));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        int exit =job.waitForCompletion(true) ? 0 : 1;

        Counter c = job.getCounters().findCounter(NGRAMS.VALUE);
        long n = c.getValue();
        if(exit==0) return n;
        else return exit;
    }

/**
 * for local run
 */
//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        run();
//    }
}
