import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class WordCountupdate1{

  public static void main(String[] arguments) 
		  throws IOException, ClassNotFoundException, InterruptedException {
    if (arguments.length != 2) {
      System.err.println("Usage: WordCountupdate1 <<<input path>>> <<<output path>>>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(WordCountupdate1.class);
    job.setJobName("Inverted Index");
   
    FileInputFormat.addInputPath(job, new Path(arguments[0]));
    FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
    
    job.setMapperClass(InvertedIndexMapper.class);
    job.setReducerClass(InvertedIndexReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);
  }
}


      class InvertedIndexMapper extends MapReduceBase
       implements Mapper<LongWritable, Text, Text, Text>{

    Text document = new Text();
    Text word = new Text();

    public void map(LongWritable key,Text value,OutputCollector<Text, Text> mapoutput,
                    Reporter reporter) 
		throws IOException, InterruptedException 
		{	
      
    	StringTokenizer itr;
    	
    	String doc = value.toString();
    	
    	String[] docarray = doc.split("\\s");
    	
    	String docid = docarray[0];
    	
    	document.set(docid);
    	
    	String temp = doc.substring(1);
        
    	itr = new StringTokenizer(temp," ");
      
        while (itr.hasMoreTokens()) {
        
        String temporary = itr.nextToken();	
        word.set(temporary);
        mapoutput.collect(word,document);
        
      }
    }
      }
  

      class InvertedIndexReducer extends MapReduceBase
       implements Reducer<Text,Text,Text,Text> {
   // private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,OutputCollector<Text, IntWritable> output,
                       Reporter reporter) 
	throws IOException, InterruptedException 
	{
      
			/*int sum = 0;
      		for (IntWritable val : values) {
      	 	 sum += val.get();
      			}
      		result.set(sum);
      		context.write(key, result);*/
		

    	Map<String,Integer> wordcounter = new HashMap<String,Integer>();
    	
    	String tempkey;
    	int previouscount, newcount;
    	for(Text i : values)
    	{
    	
    		tempkey = i.toString();
    		if(wordcounter.containsKey(tempkey))
    		{
    		previouscount = wordcounter.get(tempkey);
    		newcount = previouscount+1;
    		
    		wordcounter.put(tempkey,newcount);
    		}
    		else
          	{
    		  newcount = 1;
              	  wordcounter.put(tempkey,newcount);
    	 	 }
    	}
    	
    	int count;
    	StringBuffer bf = new StringBuffer();
    	for(String a : wordcounter.keySet())
    	{
    		bf.append(a);
    		bf.append(":");
    		count = wordcounter.get(a);
    		bf.append(count);
    		bf.append("\t");
    	}
    	
    	String document = bf.toString(); 
    	Text doc = new Text();
		doc.set(document);
		output.collect(key, doc);	
    	
    	
    }
  }
      