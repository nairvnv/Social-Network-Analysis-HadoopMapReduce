import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;


public class InMemoryReducer{
	static HashMap<String, String> user_map;


	public static class friendmap extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable id = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			

			String line[]=value.toString().split("\t");
			
			id.set(Long.parseLong(line[0]));
			
			if(line.length==2){
				String friends=line[1];   
				context.write(id, new Text("friends\t"+friends.toString()));
				}

		}
	}



	public static class usermap extends Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable iden=new LongWritable();
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException
		{

			String input[]=value.toString().split(",");
			if(input.length==10)
			{
				iden.set(Long.parseLong(input[0]));
				String year=(input[9].split("/"))[2];
				
				int yr = Integer.parseInt(year);
				context.write(iden, new Text("Details\t"+yr));				
			}
		}

	}

	public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
		
		
		static HashMap<String, String> user_map;
		static HashMap<String, String> year_map;
		private ArrayList<String> array_date = new ArrayList<String>();

		
		public void setup(Context context) throws IOException{

			Configuration config = context.getConfiguration();
			user_map = new HashMap<String, String>();
			String io_path =config.get("user_map");
			
			
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://localhost:9000"+io_path);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			while (line != null) {
				String[] token = line.split(",");
				if (token.length == 10) {
					String vals = token[9].split("/")[2];
					user_map.put(token[0].trim(), vals);
				}
				line = br.readLine();
			}


		}
		public void reduce (LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			year_map = new HashMap<String, String>();
			array_date.clear();
			int min = 0;

			for(Text val: values)
			{
				array_date.add((val.toString().split("\t")[1]));
				year_map.put(key.toString(), (val.toString().split("\t")[1]));
			}

				for(String A:array_date)
				{
					min = 0;
					String[] friends = A.split(",");
					for (int i = 0; i<friends.length; i++) {

						int year = Integer.parseInt(user_map.get(String.valueOf(friends[i])));
						if (year  > min) {
							min = year;
						}
					}
				}
				String op="\t"+String.valueOf(2022-min);

				
				context.write(new Text(key.toString()), new Text(op));


			}
		}
	

	public static void main(String[] args) throws Exception {


		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (args.length != 3) {
			System.err.println("command: java <in> <user_map> <userout>");
			System.exit(2);
		}
		conf.set("user_map", args[1]);

		Job job = new Job(conf, "inmemory");
		job.setJarByClass(InMemoryReducer.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),TextInputFormat.class, friendmap.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, usermap.class );		
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}





