import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex_q4 {

  
  public static class MapTkn
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String LineNo = value.toString().substring(0, value.toString().indexOf(","));
      String sentence =  value.toString().substring(value.toString().indexOf(",") + 1);
      
      
      StringTokenizer words = new StringTokenizer(sentence, ",");
      while (words.hasMoreTokens()) {
    	String clean_word = words.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
    	
        word.set(clean_word);
        if(word.toString() != "" && !word.toString().isEmpty()){
          context.write(word, new Text(LineNo));
        }
      }
    }
  }
  

  public static class AggregateReduce
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      ArrayList<Integer> list = new ArrayList<Integer>();
      for(Text val:values){
        list.add(Integer.parseInt(val.toString()));
      }
      Collections.sort(list);
      context.write(key, new Text(list.toString()));
    }
  }
  
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted_index");
    job.setJarByClass(InvertedIndex_q4.class);
    job.setMapperClass(MapTkn.class);
    // Commend out this part if you want to use combiner. Mapper and Reducer input and outputs type matching might be needed in this case. 
    //job.setCombinerClass(AggregateReduce.class);
    job.setReducerClass(AggregateReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}