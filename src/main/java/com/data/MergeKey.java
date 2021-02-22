package com.data;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MergeKey implements WritableComparable {
    public Text text;
    public DoubleWritable prob;

    /**
     * sort according to prob (ascending) and then by text (ascending)
     */
    public MergeKey(){
        this.text = new Text();
        this.prob = new DoubleWritable(0.0);
    }

    public MergeKey(Text txt, DoubleWritable p){
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
        MergeKey that = (MergeKey) o;
        return Objects.equals(text, that.text) &&
                Objects.equals(prob, that.prob);
    }


    @Override
    public int compareTo(Object o) {
        MergeKey other = (MergeKey) o;
        int doubleCompare = prob.compareTo(other.prob);
        if(doubleCompare != 0) {
            return doubleCompare;
        }
        return text.compareTo(other.text);
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
