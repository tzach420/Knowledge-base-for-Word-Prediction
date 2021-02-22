package com.steps;

import com.data.AppConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CalculateVars {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        String delim = AppConfig.delim;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = danny,went,home \t 10 \t 20 --> this is the output because of the textInputFormat.
            String line = value.toString();
            String[] splitted = line.split(delim);
            String ngram=null;
            String groupAcount=null;
            String groupBcount=null;
            if(splitted[0] != null)
                ngram= splitted[0];
            if(splitted[1] != null)
                groupAcount = splitted[1];
            if(splitted[2] != null)
                groupBcount= splitted[2];


            //nr0 - he number of n-grams occuring r times in the first part of the corpus.
            //nr1 - he number of n-grams occuring r times in the second part of the corpus.
            Text reduceInputValue = new Text("1"+delim+ngram);
            context.write(new Text(groupAcount+delim+"nr0"), reduceInputValue);
            context.write(new Text(groupBcount+delim+"nr1"), reduceInputValue);


            //tr01 - total times ngrams with count r in groupA  seen in groupB
            //tr10 - total times ngrams with count r in groupB  seen in groupA
            context.write(new Text(groupAcount+delim+"tr01"),new Text(groupBcount+delim+ngram));
            context.write(new Text(groupBcount+delim+"tr10"),new Text(groupAcount+delim+ngram));
        }

    }

    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {
        String delim =AppConfig.delim;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (Text value : values) {
                String val = value.toString();
                String splitted[] = val.split(delim);
                sum+=Double.valueOf(splitted[0]);
            }
            context.write(key,new Text(sum+delim+"1"));

        }
    }

    /**
     * input example - ((30 \t nr0), [(1 /t danny went home), (1 /t danny went abroad) , (1 /t danny went shop).....])
     * output example - 30 \t nr0 \t 145 \t.
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        String delim = AppConfig.delim;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double sum = 0;
            for (Text value : values) {
                String val = value.toString();
                String splitted[] = val.split(delim);
                sum+=Double.valueOf(splitted[0]);
            }
            context.write(key,new Text(sum+delim));

        }
    }





    public static int run() throws IOException, ClassNotFoundException, InterruptedException {

        System.err.println("running calculateVars");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "CalculateVars");

        job.setJarByClass(CalculateVars.class);
        job.setMapperClass(CalculateVars.MapperClass.class);
        job.setReducerClass(CalculateVars.ReducerClass.class);
        job.setCombinerClass(CalculateVars.CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(AppConfig.task2InputFile));
        FileOutputFormat.setOutputPath(job, new Path(AppConfig.task2OutputFile));

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

