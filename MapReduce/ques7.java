import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;







    public class h1bques7 {
      
    public static class h1bMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
        public void map(LongWritable key,Text value,Context context)
        {
            try{
            String[] str =value.toString().split("\t");
          
            context.write(new Text(str[7]),new IntWritable(1));}
             catch(Exception e)
             {
                System.out.println(e.getMessage());
             }
          
          
          
          
        }
    }

    public static class h1bReducer extends Reducer<Text,IntWritable,Text,IntWritable>
     {
           IntWritable result = new IntWritable();
          
            public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
              int sum = 0;
              
                 for (IntWritable val : values)
                 {         
                    sum += val.get();    
                 }
               
              result.set(sum);            
              context.write(key, result);
            
            }
     }

        /**
         * @param args
         * @throws IOException
         * @throws IllegalArgumentException
         * @throws InterruptedException
         * @throws ClassNotFoundException
         */
        public static void main(String[] args) throws Exception{
            Configuration conf = new Configuration();
            //conf.set("name", "value");
          
         Job job=Job.getInstance(conf,  "count");
         job.setJarByClass(h1bques7.class);
         job.setReducerClass(h1bReducer.class);
         job.setMapperClass(h1bMapper.class);
         //job.setNumReduceTasks(0);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job,new Path(args[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        
        
      
          }

          
            // TODO Auto-generated method stub

        }

    /**
     * @param args
     */
