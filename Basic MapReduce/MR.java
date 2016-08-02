//Team Candidate 1: Priyanka Singh (psingh28)
//Team Candidate 2: Sahil Dureja (sahildur)


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR{

	public static class TokenizerMapper 
	extends Mapper<Object,Text,Text, IntWritable>{

		//private final static IntWritable one = new IntWritable(1);
		private Text word =new Text();

		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			LinkedList<String> unwanted=new LinkedList<String>();
			unwanted.addLast("Unknown");
			unwanted.addLast("UNKWN");
			unwanted.addLast("Arr Arr");
			unwanted.addLast("Arr");

			try{
			String line = value.toString();
			String[] tokens=line.split(",");

			String x = tokens[2].toString();
			String[] hall=x.split(" ");
			String building = hall[0];
			String sem = tokens[1];
			if(!(unwanted.contains(building) || unwanted.contains(sem))){
			String k = building+"_"+sem;
			word.set(k);
			int cap = Integer.parseInt(tokens[7]);
			if(cap>=0){
			IntWritable val = new IntWritable(cap);

			context.write(word, val);}}}
			catch(NumberFormatException e){
				
				System.out.println(e.getMessage());
				}
			

			//StringTokenizer itr = new StringTokenizer(value.toString());
			//while(itr.hasMoreTokens()){
				//word.set(itr.nextToken());
				//context.write(word, one);

			
		}
	}

	public static class IntSumReducer
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}

			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf, "word count");
		job.setJarByClass(MR.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}
}

