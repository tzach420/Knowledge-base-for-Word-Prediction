package com;
import com.steps.*;

import java.io.*;
import java.util.Scanner;


public class Main {
    public static void failed(String s) {
        System.err.println(s);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long N=0;
        if ( (N = SplitToGroups.run()) != 0) {
            if (CalculateVars.run() == 0)
                if (MergeGroupsWithVars.run() == 0) {

                    if (CalculateProb.run(N) == 0)
                        if (Sort.run() == 0)
                            System.out.println("done");
                        else failed("Sort failed");

                    else failed("Calculate prob failed");
                }
                else failed("Merge failed");
            else failed("CalculateVars failed");
        }
        else failed("Split failed");

    }
}
