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
import java.util.LinkedList;
import java.util.List;


public class CalculateProb {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        String delim = AppConfig.delim;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input example - (danny went home\t sum  \t NR0/NR1/TR01/TR10)
            String[] splitted= value.toString().split(delim);
            Text reducerKey= new Text(splitted[0]);
            Text reducerVal= new Text(splitted[2]+delim+splitted[1]);
            context.write(reducerKey,reducerVal);
        }

    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        String delim = AppConfig.delim;
        long N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("N",1);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            //input (danny went home, [(n0,14), (n1,22)...])
            double tr01=-1,tr10=-1,nr0=-1,nr1=-1;
            List<Text> list =new LinkedList<>();
            for(Text t : values){
                list.add(new Text(t));
            }
            for(Text text: list){
                String[] splitted= text.toString().split(delim);
                String type=splitted[0];
                double count=Double.valueOf(splitted[1]);
                if(type.equals("nr0")) nr0=count;
                else if (type.equals("nr1")) nr1= count;
                else if(type.equals("tr01")){
                    tr01 =count;
                    if(count==2145.0)
                        System.out.println("hlello");
                }
                else if(type.equals("tr10")) tr10 =count;
            }
            if(nr0 == -1 || nr1 == -1 || tr01 == -1 || tr10 == -1) {
                throw new IOException("problem with nr0/nr1/tr01/tr10");
            }
            double prob = (tr01+tr10)/(N*(nr0+nr1));
            context.write(key,new Text(Double.toString(prob)));
        }
    }





    public static int run(long n) throws IOException, ClassNotFoundException, InterruptedException {
        System.err.println("running calculateProb with N:"+ n);
        Configuration conf = new Configuration();
        conf.setLong("N", n);
        Job job = new Job(conf, "CalculateProb");
        job.setJarByClass(CalculateProb.class);
        job.setMapperClass(CalculateProb.MapperClass.class);
        job.setReducerClass(CalculateProb.ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(AppConfig.task3InputFile));
        FileOutputFormat.setOutputPath(job, new Path(AppConfig.task3OutputFile));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        int exit =job.waitForCompletion(true) ? 0 : 1;
        return exit;
    }
/**
 * for local run
 */
//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        run(1000);
//    }

}
