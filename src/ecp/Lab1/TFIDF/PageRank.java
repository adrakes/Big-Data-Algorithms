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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

import ecp.Lab1.TFIDF.TFIDF.Order;
import ecp.Lab1.TFIDF.TFIDF.rankCompare;


public class PageRank extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new PageRank(), args);
	      
	      System.exit(res);
	   }
	
	private static final String OUTPUT_PATH = "pagerank/intermediate_output";

	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "PageRank");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job.setJarByClass(PageRank.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(Map1.class);
	      job.setReducerClass(Reduce1.class);
	      

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("pagerank/input")); 
	      //Path outputPath = new Path("output/PageRank");
	      //FileOutputFormat.setOutputPath(job, outputPath);
	      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH)); 
	      FileSystem hdfs = FileSystem.get(getConf());
	      Path intoutputPath = new Path("pagerank/intermediate_output");
	      if (hdfs.exists(intoutputPath)){
		      hdfs.delete(intoutputPath, true);
	      }


		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	  
	      
	      Job job2 = new Job(getConf(), "PageRank");
	      job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job2.setJarByClass(PageRank.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);
	      
	      job2.setMapperClass(Map2.class);
	      job2.setReducerClass(Reduce2.class);
	      

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH)); 
	      Path outputPath = new Path("pagerank/output");
	      FileOutputFormat.setOutputPath(job2, outputPath); 
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job2.waitForCompletion(true);
	      
	      BufferedReader reader = new BufferedReader(new FileReader("/Users/andreasveen/Documents/workspace/WordCount/pagerank/output/part-r-00000"));	         
	        //Creating ArrayList to hold Order objects
	        
	        ArrayList<Order> Records = new ArrayList<Order>();	         
	        //Reading Order records one by one	         
	        String currentLine = reader.readLine();	         
	        while (currentLine != null)
	        {
	            String[] splitLine = currentLine.split(";");	             
	            String wordDoc = splitLine[0];
	            Float rank = Float.valueOf(splitLine[1]);	 
	            //Creating Order object for every student record and adding it to ArrayList	             
	            Records.add(new Order(wordDoc, rank));	 
	            currentLine = reader.readLine();
	        }
	        
	        //Sorting ArrayList studentRecords based on rank           
	        Collections.sort(Records, new rankCompare()); 
	        //Creating BufferedWriter object to write into output text file 
	        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/andreasveen/Documents/workspace/WordCount/pagerank/part-r-00000-sorted"));         
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
	      

		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));3
		  
	      
	   

	 public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
	      private String firstNode = new String();
	      private String secondNode = new String();
	      private static Set<String> nodes = new HashSet<String>();

	      

	     
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 		if (value.charAt(0) !='#' ){
	    	 			firstNode = (value.toString().split("\t+")[0]);
	    	 			secondNode = (value.toString().split("\t+")[1]);
	    	 			nodes.add(firstNode);
	    	 			nodes.add(secondNode);
	    	 		}
	    	 		if(!firstNode.equals("") && firstNode.length() !=0){
	           	context.write(new Text(firstNode), new Text(secondNode));
	    	 		}
	           	//System.out.println(firstNode + secondNode);
	           	//System.out.println(nodes);
	            
	    	 		
	       }
	   }
	 
	 public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
			 Double dampingFactor = 0.85;
			 Double pageRank = (dampingFactor/Map1.nodes.size());	
			 String line = new String();
			 for (Text verdi : values){
				 line += verdi+",";
			 }
			 line = line.substring(0, line.length()-1);
			 String value = pageRank.toString() + ";" + line;
	         context.write(key, new Text (value));
	    	 
	      }
	}
	 public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    	 
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {	    	 
	    	 String node = (value.toString().split(";")[0]);
    		 String pageRank = (value.toString().split(";")[1]);
	    	 String connections = (value.toString().split(";")[2]);  
	    	 String[] connectedNodes = connections.split(",");

	    	 for (String otherNode : connectedNodes){
	    		 Text numLinks = new Text (pageRank + ";" + connectedNodes.length);
	    		 context.write(new Text(otherNode), new Text("#" + numLinks)); 
	    		 //System.out.println(numLinks);	    		 
	    	 }	    	 
	           	context.write(new Text(node), new Text(connections));	  
	           	
	            }
	     
	       }
	   
	 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
			 String innhold = new String();
			 String info = new String();
			 String nodes = new String();
			 Double newPageRank = 0.0;
			 Double dampingFactor = 0.85;
			 for (Text verdi : values) {
				 innhold = verdi.toString();			 
				 if (innhold.startsWith("#")){
					 info = innhold.substring(1);
					 Double pageRank = Double.parseDouble(info.split(";")[0]);	
					 Integer numOfLinks = Integer.parseInt(info.split(";")[1]);
					 newPageRank += (pageRank/numOfLinks);
				 }
				 else{				 
					 nodes += innhold;
					
				 }
				 }
				 Double pageRankIteration = dampingFactor * newPageRank + (1-dampingFactor);
				 
				 context.write(key, new Text(pageRankIteration + ";" + nodes));
				 
	      }
			
	    	 
	      }
	
	}
	 
	
	 
	 

