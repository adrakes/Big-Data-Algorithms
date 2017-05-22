package ecp.Lab1.TFIDF;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class TFIDF extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new TFIDF(), args);
	      
	      System.exit(res);
	   }
	
	private static final String OUTPUT_PATH = "intermediate_output";
	private static final String OUTPUT_PATH2 = "intermediate_output2";
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "TFIDF");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job.setJarByClass(TFIDF.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);
	      
	      job.setMapperClass(Map1.class);
	      job.setReducerClass(Reduce1.class);
	      

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("tfidf/input")); 
	      //Path outputPath = new Path("output/TFIDF");
	      //FileOutputFormat.setOutputPath(job, outputPath);
	      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH)); 
	      FileSystem hdfs = FileSystem.get(getConf());
	      Path intoutputPath = new Path("tfidf/intermediate_output");
	      if (hdfs.exists(intoutputPath)){
		      hdfs.delete(intoutputPath, true);
	      }


		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	  
	      
	      Job job2 = new Job(getConf(), "TFIDF");
	      job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job2.setJarByClass(TFIDF.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);
	      
	      job2.setMapperClass(Map2.class);
	      job2.setReducerClass(Reduce2.class);
	      

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH)); 
	      FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH2)); 
	      Path int2outputPath = new Path("tfidf/intermediate_output2");
		  if (hdfs.exists(int2outputPath)){
		      hdfs.delete(int2outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job2.waitForCompletion(true);
	      
	      
	      Job job3 = new Job(getConf(), "TFIDF");
	      job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job3.setJarByClass(TFIDF.class);
	      job3.setOutputKeyClass(Text.class);
	      job3.setOutputValueClass(Text.class);
	      
	      job3.setMapperClass(Map3.class);
	      job3.setReducerClass(Reduce3.class);
	      

	      job3.setInputFormatClass(TextInputFormat.class);
	      job3.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH2)); 
	      Path outputPath = new Path("tfidf/output/TFIDF");
	      FileOutputFormat.setOutputPath(job3, outputPath); 
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));3
		  
	      job3.waitForCompletion(true);
	      //Creating BufferedReader object to read the input text file
	
	        BufferedReader reader = new BufferedReader(new FileReader("/Users/andreasveen/Documents/workspace/WordCount/tfidf/output/TFIDF/part-r-00000"));	         
	        //Creating ArrayList to hold Order objects
	        
	        ArrayList<Order> Records = new ArrayList<Order>();	         
	        //Reading Order records one by one	         
	        String currentLine = reader.readLine();	         
	        while (currentLine != null)
	        {
	            String[] splitLine = currentLine.split(";");	             
	            String wordDoc = splitLine[0] + ";" + splitLine[1];
	            Float rank = Float.valueOf(splitLine[2]);	 
	            //Creating Order object for every student record and adding it to ArrayList	             
	            Records.add(new Order(wordDoc, rank));	 
	            currentLine = reader.readLine();
	        }
	        
	        //Sorting ArrayList studentRecords based on rank           
	        Collections.sort(Records, new rankCompare()); 
	        //Creating BufferedWriter object to write into output text file 
	        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/andreasveen/Documents/workspace/WordCount/tfidf/output/TFIDF/part-r-00000-sorted"));         
	        //Writing every studentRecords into output text file
	         
	        for (Order order : Records) {
	            writer.write(order.wordDoc);
	            writer.write(";"+order.rank);	             
	            writer.newLine();
	        }
	         
	        //Closing the resources	         
	        reader.close();	         
	        writer.close();	 
            
	        return 0 ;
	      
	   }
	class Order{
	    String wordDoc;     
	    float rank;
	     
	    public Order(String wordDoc, float rank) 
	    {
	        this.wordDoc = wordDoc; 
	        this.rank = rank;  
	    }
	}
	 
	//nameCompare Class to compare the names
	 
	class wordDocCompare implements Comparator<Order>{
	    @Override
	    public int compare(Order s1, Order s2) {
	        return s1.wordDoc.compareTo(s2.wordDoc);
	    }
	}
	 
	//rankCompare Class to compare the rank
	 
	class rankCompare implements Comparator<Order>
	{
	    @Override
	    public int compare(Order s1, Order s2)
	    {	
	    	int sorter= 1;
	    	if (s2.rank <= s1.rank){
	    		sorter= -1;
	    	}
	    	
	    return sorter;
	    }
	}

	 public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private Text word = new Text();
	      private Text fileName = new Text();
	      private final static IntWritable ONE = new IntWritable(1);
	      

	     
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 	for (String token: value.toString().toLowerCase().replaceAll("[^0-9A-Za-z ]","").split("\\s+")){
		        word.set(token);	
		        if(!word.equals("") && word.getLength() !=0){     
	           	String nameOfFile = ((FileSplit) context.getInputSplit()).getPath().getName();
	           	fileName = new Text(nameOfFile);
	           	String combo = word + ";" + fileName;
	           	context.write(new Text(combo), ONE);
		        }
	            }
	       }
	   }

	 public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		 @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
			 
			 int sum = 0;
	         for (IntWritable val : values) {
	             sum += val.get();
	          }
	          context.write(key, new IntWritable(sum));
	    	 
	      }
	}
	 public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    	 
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 String word = (value.toString().split(";")[0]);
    		 String doc = (value.toString().split(";")[1]);
	    	 Integer frequency = Integer.parseInt(value.toString().trim().split(";")[2]);  

	    	 String counts = word + ";" + frequency;
	
	           	context.write(new Text(doc), new Text(counts));
	            }
	     
	       }
	   
	 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
			 HashSet<String> hashset = new HashSet<String>();
			 String doc = key.toString();
			 Integer count = 0;
			 Integer frequency = 0;
			 String word = new String();
			 ArrayList<String> cache = new ArrayList<String>();
			 for (Text value : values){
				 frequency = Integer.parseInt(value.toString().split(";")[1]);
				 cache.add(value.toString());
				 count+=frequency;
			 }
			 
			 for (String val : cache){
				 word = val.split(";")[0];
				 frequency = Integer.parseInt(val.split(";")[1]);
				 context.write(new Text(word + ";" + doc), new Text(frequency + ";" + count));
			 }
	      }
			
	    	 
	      }
public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
    	 
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 String word = (value.toString().split(";")[0]);
    		 String doc = (value.toString().split(";")[1]);
	    	 Integer frequency = Integer.parseInt(value.toString().trim().split(";")[2]);  
	    	 Integer count = Integer.parseInt(value.toString().trim().split(";")[3]); 

	    	 String counts = doc + ";" + frequency + ";" + count;
	    	 
	    	 
	           	context.write(new Text(word), new Text(counts));
	            }
	     
	       }
	   
	 public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
			 HashSet<String> docCount = new HashSet<String>();
	    	 Double docs = 0.00;
			 String word = key.toString();
			 Double counts = 0.00;
			 Integer frequency = 0;
			 Integer docsPerWord = 0;
			 Double tfidf = 0.00;
			 String doc = new String();
			 ArrayList<String> cache = new ArrayList<String>();
			 
			 for (Text value : values){
				 doc = value.toString().split(";")[0];
				 docsPerWord++;
				 cache.add(value.toString());
				 if(!docCount.contains(doc)){
		    		 docCount.add(doc);
		    		 docs++;
		    	 }
			 }
			 
			 for (String val : cache){
				 doc = val.split(";")[0];
				 frequency = Integer.parseInt(val.split(";")[1]);
				 counts = Double.parseDouble(val.split(";")[2]);
				 tfidf = (frequency/counts)+Math.log(docs/docsPerWord);
				 //System.out.println(frequency + " " + counts + " " + docs + " " + docsPerWord);
				 context.write(new Text(word+";"+doc), new Text(String.format("%.12f", tfidf)));
			 }
			 
	      }
			
	    	 
	      }
	
	}
	 
	

