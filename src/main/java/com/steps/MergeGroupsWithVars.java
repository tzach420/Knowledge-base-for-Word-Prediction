package com.steps;

import com.data.AppConfig;
import com.data.MergeKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MergeGroupsWithVars {
    public static class MapperClass extends Mapper<LongWritable, Text, MergeKey, Text> {
        String delim = AppConfig.delim;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = danny went home \t 10 \t 20 || 30 \t nr0 \t 145
            String groupAtag = "a";
            String groupBtag = "b";
            String line = value.toString();
            String[] splitted = line.split(delim, -1);
                if (splitted.length == 3) {
                    //input danny went home \t 10 \t 20
                        Double numOfOcc = Double.valueOf(splitted[1]) + Double.valueOf(splitted[2]);
                        MergeKey mergeKey = new MergeKey(new Text(groupBtag), new DoubleWritable(numOfOcc));
                        context.write(mergeKey, new Text(splitted[0]));
                        //output (mergekey: 30 , b), (danny went home)

                } else if (splitted.length == 4) {
                    //input 30 \t nr0 \t 145
                    MergeKey mergeKey = new MergeKey(new Text(groupAtag), new DoubleWritable(Double.valueOf(splitted[0])));
                    context.write(mergeKey, new Text(splitted[2] + "\t" + splitted[1]));
                    //output (30 \t a) , (145 \t nr0)
                }

        }

    }

    /**
     * input example - [mergekey:(30 \t a)], [140 \t nr0, 120 \t nr1, 150 \t tr0, 70 \t tr1]) - in this case always at size 4.
     *                 [mergekey:(30 \t b)], [danny went home, danny love cakes....] - we dont know the size.
     * output example - danny went home , 140 nro
     */
    public static class ReducerClass extends Reducer<MergeKey, Text, Text, Text> {
        double lastId;
        List<Text> list;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            list = new LinkedList<>();
            lastId=-1.0;
        }
        public void reduce(MergeKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double tag= key.getProb().get();
            if(tag!=lastId) {// table one (nro/nr1/...)
               list.clear();
               for(Text t : values){
                   list.add(new Text(t.toString()));
               }

            }
            else { // table two (ngrams)
                if (list.size() == 4) {
                    for (Text val : values) {
                        for (Text val2 : list) {
                            context.write(val, val2);
                            //output example - danny went home , 140 \t nr0
                        }
                    }
                }
                else{
                    System.out.println("hello");
                }

            }
            lastId = tag;
        }
    }

    public static class PartitionerClass extends Partitioner<MergeKey,Text> {

        @Override
        public int getPartition(MergeKey key, Text value, int numPartitions) {
            return (key.getProb().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }





    public static int run() throws IOException, ClassNotFoundException, InterruptedException {
        System.err.println("running mergeGroupsWithVars");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MergeGroupsWithVars");

        job.setJarByClass(MergeGroupsWithVars.class);
        job.setMapperClass(MergeGroupsWithVars.MapperClass.class);
        job.setReducerClass(MergeGroupsWithVars.ReducerClass.class);
        job.setPartitionerClass(MergeGroupsWithVars.PartitionerClass.class);

        job.setMapOutputKeyClass(MergeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(AppConfig.task25InputFile));
        FileInputFormat.addInputPath(job, new Path(AppConfig.task2InputFile));
        FileOutputFormat.setOutputPath(job, new Path(AppConfig.task25OutputFile));

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
