/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blogclustermr;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.*;

/**
 *
 * @author matyi
 */
public class EdgeLister {

    public static class EdgeMapper extends Mapper<Object, Text, Text, LongWritable> {

        private Text edgePair = new Text();
        private final LongWritable one = new LongWritable(1);
        private JSONObject jsonObj;

       
        
        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            try {
                Set<String> blogs = new HashSet<>();
                JSONObject obj = new JSONObject(value.toString());
                JSONArray arr = obj.getJSONArray("likes");
                for (int i = 0; i < arr.length(); i++)
                {
                    String blogId = arr.getJSONObject(i).getString("blog");
                    blogs.add(blogId);
                }
                if(blogs.size() > 1)
                {
                    Object[] ids = blogs.toArray();
                    for(int i = 0; i < ids.length-1; i++)
                    {
                        for(int j = i+1; j < ids.length; j++)
                        {
                            String newKey;
                            String v1 = (String)ids[i];
                            String v2 = (String)ids[j];
                            
                            if(v1.compareTo(v2) > 0)
                            {
                                newKey = v1+", "+v2+",";
                            }
                            else
                            {
                                newKey = v2+", "+v1+",";
                            }
                            edgePair.set(newKey);
                            context.write(edgePair, one);
                        }
                    }
                }
            } catch (JSONException e) {
                // Hmm unable to Parse the JSON, off to next record, better log though :-)
            }
        }
    }

    public static class EdgeWeightReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws InterruptedException, IOException {

            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            if(sum > 9)
            {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job;
        job = Job.getInstance(conf, "edge_lister");
        job.setJarByClass(EdgeLister.class);
        job.setMapperClass(EdgeMapper.class);
        //job.setCombinerClass(EdgeWeightReducer.class);
        job.setReducerClass(EdgeWeightReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
