import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;

public class ques1a {
    public static class myMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable>
    {
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
        {
            String[] arr = value.toString().split("\t");
            String jobtitle = arr[4];
            int year = Integer.parseInt(arr[7]);
            if(jobtitle.equals("DATA ENGINEER"))
            context.write(new IntWritable(year),new LongWritable(1));
        }
    }
  
    public static class myReducer extends Reducer<IntWritable, LongWritable, NullWritable, Text>
    {
        TreeMap<Integer, Long> tmap = new TreeMap<Integer, Long>();
        static long temp = 0;
        public void reduce(IntWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException
        {
            long sum=0;
            for (LongWritable val : value)
            {
                sum+= val.get();
            }
            //context.write(key, new IntWritable(sum));
            tmap.put(key.get(), sum);
        }
      
        @SuppressWarnings("rawtypes")
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            double growthperc = 0.0, sumgrowth = 0.0;
            int count = 0;
            //long temp = 0;  
          
            for( Map.Entry m:tmap.entrySet())
            {
                int mapKey = (int) m.getKey();
                long mapValue = (long) m.getValue();
                String tempResult = "";
              
                if(temp != 0)
                {      
                    growthperc = (double)(mapValue - temp) * 100 /temp;
                    tempResult = String.format("%.2f", growthperc);
                    temp = mapValue;
                    sumgrowth+=growthperc;                  
                    context.write(NullWritable.get(), new Text("" + mapKey + "\t" + mapValue + "\t\t        " + tempResult + " %"));      
                }
                else
                {
                    String tempString  = "Year" + "\t" + "Total_Application" + "\t" + "Growth(%)" + "\n" + mapKey + "\t" + mapValue;
                    temp = mapValue;
                    context.write(NullWritable.get(), new Text(tempString));      
                }
                count++;
            }
            String tempResult = "\n\n Average Growth -> " + String.format("%.2f", sumgrowth/count) + " %";
            context.write(NullWritable.get(), new Text(tempResult));      
        }
    }
  
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
      
        job.setJarByClass(ques1a.class);
        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
              
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
  
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
      
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0: 1);
      
    }
}
