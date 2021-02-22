package com;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.data.AppConfig;

import java.io.IOException;

public class LocalRunner {
    public static StepConfig makeConfig(String className) {
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig();
        hadoopJarStep.withJar(AppConfig.jarPath)
                .withMainClass(className);

        StepConfig stepConfig = new StepConfig()
                .withName("hadoop" + className)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        return stepConfig;

    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            System.err.println("running localRunner");
            AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.defaultClient();
            StepConfig stepConfig = makeConfig("ass2-Main");


            JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                    .withInstanceCount(AppConfig.instanceCount)
                    .withMasterInstanceType(AppConfig.instanceType)
                    .withSlaveInstanceType(AppConfig.instanceType)
                    .withHadoopVersion(AppConfig.hadoopVer)//.withEc2KeyName(AppConfig.keyPair)
                    .withKeepJobFlowAliveWhenNoSteps(false)
                    .withPlacement(AppConfig.placementType);


            RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                    .withName("ass2")
                    .withInstances(instances)
                    .withSteps(stepConfig)
                    .withLogUri(AppConfig.logUri)
                    .withJobFlowRole("EMR_EC2_DefaultRole")
                    .withServiceRole("EMR_DefaultRole")
                    .withReleaseLabel("emr-6.2.0");

            RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
            String jobFlowId = runJobFlowResult.getJobFlowId();
            System.out.println("Ran job flow with id: " + jobFlowId);
    }

}

