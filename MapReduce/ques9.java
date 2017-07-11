import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



import java.io.*;

public class ans9 {
   
    public static class h1bMapper1 extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException
        {
            String[] abc = value.toString().split("\t");
            context.write(new Text(abc[2]), value);
           
           
           
           
        }
    }
    public static class h1breducer1 extends Reducer<Text,Text,Text,FloatWritable>
    {
        public void reduce(Text key,Iterable<Text> value,Context context)throws IOException, InterruptedException
        {int count = 0,count1 = 0,count2 = 0;
        float success;
        long myval = 0 ;
        for(Text t:value)
        {
            String parts[] = t.toString().split("\t");
            count++;
            if(parts[1].equals("CERTIFIED"))
            {
                count1++;
            }
            if(parts[1].equals("CERTIFIED-WITHDRAWN"))
            {
                count2++;
            }
        }
   
       
        if(count > 1000)
        {
            success = ((count1+count2)*100)/count;
            if(success > 70.0)
            {
            myval = (long) success;
            }
        context.write(key,new FloatWritable(myval));
        }
    }
   
}
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //conf.set("name", "value");
      
     Job job=Job.getInstance(conf,  "count");
     job.setJarByClass(ans9.class);
     job.setReducerClass(h1breducer1.class);
     job.setMapperClass(h1bMapper1.class);
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(Text.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(FloatWritable.class);
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job,new Path(args[1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }

}
