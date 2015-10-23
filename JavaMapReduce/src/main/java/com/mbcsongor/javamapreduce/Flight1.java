package com.mbcsongor.javamapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Melyik reptéren gurulnak (TaxiIn és TaxiOut) átlagosan legtöbbet a gépek?
public class Flight1 {
    
    public static final int DestAirportIndex  = 17;
    public static final int TaxiInIndex  = 19;
    public static final int OriginAirportIndex  = 16;
    public static final int TaxiOutIndex = 20;
    
    public static class TaxiMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable taxiTime = new IntWritable(0);
    private Text airport = new Text();


    public void map(Object key, Text value, Reducer.Context context
                    ) throws IOException, InterruptedException {
      String[] values = value.toString().split(",");
      
        if(setOutput(values,DestAirportIndex,TaxiInIndex))
        {
            context.write(airport, taxiTime);
        }
        if(setOutput(values,OriginAirportIndex,TaxiOutIndex))
        {
            context.write(airport, taxiTime);
        }
    }
    
    public boolean setOutput(String[] values,int airportIndex,int timeIndex)
    {
        if(values.length <= airportIndex || values.length <= timeIndex)
            return false;
        if(values[airportIndex].equals("NA") || values[timeIndex].equals("NA"))
            return false;
        try{
            Integer taxiT = Integer.parseInt(values[timeIndex]);
            taxiTime.set(taxiT);
            airport.set(values[airportIndex]);
        }
        catch(Exception e){
            return false;
        }
        return true;
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();


    public void reduce(Text key, Iterable<IntWritable> values,
                       Reducer.Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int counter = 0;
      for (IntWritable val : values) {
        sum += val.get();
        counter++;
      }
      result.set(sum / counter);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "flight1");
    job.setJarByClass(Flight1.class);
    job.setMapperClass(TaxiMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
