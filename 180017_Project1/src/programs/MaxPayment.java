package programs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import programs.MaxPayment;


public class MaxPayment {

	public static class MapForWordCount extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
		{
			String line= value.toString();
			String[] words=line.split(",");

			Text outputkey=new Text(words[3]);
			IntWritable outputvalue=new IntWritable(1);
			con.write(outputkey,outputvalue);
			
		}
		
	}
	
	//reducer class
	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    Map<Text, IntWritable> hashMap;
	    
	    public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
	      
	      if (hashMap == null) {
	        hashMap = new HashMap<Text, IntWritable>();
	      }
	      
	      int sum = 0;
	      for (IntWritable value: values) {
	        sum = sum+value.get();
	      }
	      
	      hashMap.put(word, new IntWritable(sum));
//	      con.write(word, new IntWritable(sum));
	    }
	    
	    public void cleanup(Context con) throws IOException, InterruptedException {
	      
	      Text highestKey = new Text("");
	      IntWritable highestValue = new IntWritable(Integer.MIN_VALUE);
	      
	      for (Map.Entry<Text, IntWritable> entry : hashMap.entrySet()) {
	        if (entry.getValue().get() >= highestValue.get()) {
	          highestKey = entry.getKey();
	          highestValue = entry.getValue();
	        }
	      }
	      
	      String finalKeyString = "Most used card : " + highestKey.toString();
	      
	      con.write(new Text(finalKeyString), highestValue);
	    }
	    
	  }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration c= new Configuration();
		Job j=Job.getInstance(c,"payment types");
		j.setJarByClass(MaxPayment.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j,new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	

}
