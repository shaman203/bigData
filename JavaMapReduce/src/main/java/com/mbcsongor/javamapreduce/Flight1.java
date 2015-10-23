package com.mbcsongor.javamapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Melyik reptéren gurulnak (TaxiIn és TaxiOut) átlagosan legtöbbet a gépek?
public class Flight1 {

    public static final int DestAirportIndex = 17;
    public static final int TaxiInIndex = 19;
    public static final int OriginAirportIndex = 16;
    public static final int TaxiOutIndex = 20;
    public static final String tempPath = "temp/";

    
    public static class TaxiMapper extends Mapper<Object, Text, Text, LongWritable> {

        private Text airport = new Text();
        private LongWritable taxiTime = new LongWritable(0);
       

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            if (setOutput(values, DestAirportIndex, TaxiInIndex)) {
                context.write(airport, taxiTime);
            }
            if (setOutput(values, OriginAirportIndex, TaxiOutIndex)) {
                context.write(airport, taxiTime);
            }
        }

        public boolean setOutput(String[] values, int airportIndex, int timeIndex) {
            if (values.length <= airportIndex || values.length <= timeIndex) {
                return false;
            }
            if (values[airportIndex].trim().equals("NA") || values[timeIndex].trim().equals("NA")) {
                return false;
            }
            try {
                Integer taxiT = Integer.parseInt(values[timeIndex].trim());
                taxiTime.set(taxiT);
                airport.set(values[airportIndex]);
            } catch (Exception e) {
                return false;
            }
            return true;
        }

    }

    public static class IntAvgReducer
            extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        
            
            long sum = 0;
            double counter = 0.0;
            for (LongWritable val : values) {
                sum += val.get();
                counter++;
            }
            double r = sum / counter;
            result.set(r);
            context.write(key, result);
        }
    }

    public static class AirportTimeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable one = new IntWritable(1);
            
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one,value);
        }


    }

    public static class MaxReducer
            extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        private DoubleWritable taxiTime = new DoubleWritable();
        private Text airport = new Text();
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
            double maxTime = 0.0;
            String slowestPort = "";
            for(Text value : values)
            {
              String[] parts = value.toString().split("[ \t]");
              double time = Double.parseDouble(parts[1]);
              if(time > maxTime)
              {
                 maxTime = time;
                 slowestPort = parts[0];
              }
            }
            taxiTime.set(maxTime);
            airport.set(slowestPort);
            context.write(airport, taxiTime);
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job;
        job = Job.getInstance(conf, "flight1_mean");
        job.setJarByClass(Flight1.class);
        job.setMapperClass(TaxiMapper.class);
        job.setReducerClass(IntAvgReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tempPath));
        job.waitForCompletion(true);
       
        job = Job.getInstance(conf, "flight1_max");
        job.setJarByClass(Flight1.class);
        job.setMapperClass(AirportTimeMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(tempPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
