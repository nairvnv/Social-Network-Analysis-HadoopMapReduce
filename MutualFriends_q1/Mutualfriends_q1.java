import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;


public class Mutualfriends_q1{

	public static class Map 
			extends Mapper<LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text mutual_friends = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			String[] split=value.toString().split("\\t");
			String user_Id=split[0];
			if(split.length==1) {
				return;
			}
			String[] ids=split[1].split(",");
			
			for(String friend : ids) {
				if(user_Id.equals(friend)) {
					continue;
				}
			String userKey = (Integer.parseInt(user_Id) < Integer.parseInt(friend))?user_Id + "," +friend : friend + ","+ user_Id;
			String regex="((\\b"+ friend + "[^\\w]+)|\\b,?" + friend + "$)";
			mutual_friends.set(split[1].replaceAll(regex, ""));
			user.set(userKey);
			context.write(user,mutual_friends);
			}
		}
	}
		
	public static class Reduce
			extends Reducer<Text, Text, Text, Text>{
		
		private String matchingFriends(String firstList, String secondList) {
			
			if(firstList == null || secondList == null) {
				return null;
			}
			
			//declarations of hash sets and String list
			LinkedHashSet<String> match1 = new LinkedHashSet();
			LinkedHashSet<String> match2 = new LinkedHashSet();
			String[] list1=firstList.split(",");
			String[] list2=secondList.split(",");
			

			//adding users to sets
			for(String  user: list1) {
				match1.add(user);
			}
			for(String  user: list2) {
				match2.add(user);
			}
			
			match1.retainAll(match2);
			String output =match1.toString().replaceAll("\\[|\\]", "");
			
			return output;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
			
			String[] list = new String[2];
			int count=0;
			
			for(Text value:values) {
				list[count++] = value.toString();
			}
			String mutuals = matchingFriends(list[0],list[1]);
			if( mutuals != null && mutuals.length() != 0) {
				context.write(key, new Text(mutuals));
			}
		}
		
	}

	


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("error in parameters");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutual friends");
		job.setJarByClass(Mutualfriends_q1.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}