package com.data;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;

public class AppConfig {
    public static long N = 1000;
    public static String delim = "\t";
    public static String ngramFilePath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static String bucketName = "s3://dspass2buckettzachandamir";
    //public static String bucketName = "/Users/tzach/Desktop/univ/dsp/ass2";
    public static String jarPath=bucketName+"/aws-jar-with-dependencies.jar";
    //public static String ngramFilePath = bucketName+"/input";

    public static String task1InputFile = ngramFilePath;
    public static String task1OutputFile = bucketName+"/output1";

    public static String task2InputFile = bucketName+"/output1";
    public static String task2OutputFile = bucketName+"/output2";

    public static String task25InputFile = bucketName+"/output2";
    public static String task25OutputFile = bucketName+"/output25";

    public static String task3InputFile = bucketName+"/output25";
    public static String task3OutputFile = bucketName+"/output3";

    public static String task4InputFile= bucketName+"/output3";
    public static String task4OutputFile= bucketName+"/output4";

    //hadoop configurations:
    public static int instanceCount=8;
    public static String instanceType= InstanceType.M4Large.toString();
    public static String hadoopVer = "3.2.1";
    public static PlacementType placementType= new PlacementType("us-east-1a");
    public static String logUri=bucketName+"/logs";

}
