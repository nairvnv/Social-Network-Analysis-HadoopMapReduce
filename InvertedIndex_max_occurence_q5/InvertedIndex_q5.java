import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndex_q5 {

  
  public static class MapTkn
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String occurences=value.toString();
      //int count = occurences.length() - occurences.replace(",", "").length();
      String[] name = value.toString().split("\t");
      word.set(name[0]);
      name[1].replace("[","").replace("]","");
      int count = name[1].split(",").length;
      for (int i = 0; i < count; i++) {
    	  context.write(word, one);
      }
    }
  }
  

  public static class AggregateReduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	private IntWritable result = new IntWritable(0);	
    private String finalkey="";
      
	
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int counter=0;
    	for(IntWritable value : values) {
    		counter+=value.get();
    	}
    	if(counter>result.get()) {
    		result.set(counter);
			finalkey=key.toString();
		}
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(finalkey) , result);
    }
  }
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex_q5.class);
    job.setMapperClass(MapTkn.class); 
    job.setCombinerClass(AggregateReduce.class);
    job.setReducerClass(AggregateReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}