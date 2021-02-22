package com.data;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable {
    public Text text;
    public DoubleWritable prob;

    public MyKey(){
        this.text = new Text();
        this.prob = new DoubleWritable(0.0);
    }

    public MyKey(Text txt, DoubleWritable p){
        this.text = txt;
        this.prob = p;

    }

    public Text getText() {
        return text;
    }

    public DoubleWritable getProb() {
        return prob;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MyKey other = (MyKey) o;

        if(text == null && other.text != null) return false;
        if(text != null && other.text == null) return false;
        if(prob != null && other.prob == null) return false;
        if(prob != null && other.prob == null) return false;
        //both != null
        return text.equals(other.text) && prob.equals(other.prob);
    }


    @Override
    public int compareTo(Object o) {
        MyKey other = (MyKey) o;
        int textCompare = text.compareTo(other.text);
        if(textCompare != 0) {
            return textCompare;
        }
        return other.prob.compareTo(prob);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        text.write(dataOutput);
        prob.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        text.readFields(dataInput);
        prob.readFields(dataInput);
    }

    @Override
    public String toString() {
        return text.toString() + "\t" + prob.get();
    }
}

