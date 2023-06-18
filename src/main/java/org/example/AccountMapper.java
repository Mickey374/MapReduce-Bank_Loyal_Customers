package org.example;

import java.io.IOException;

//Import the Hadoop libraries
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class AccountMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value, Context c) throws IOException, java.lang.InterruptedException{
        String line = value.toString();
        String[] words  = line.split(",");

        Text id = new Text(words[0]);

        Text account = new Text("A,"+ words[1] + "," + words[3] + ","+ words[4]+ ","+ words[5]);
        c.write(id, account);
    }
}
