package ecp.bigdata.Tutorial1;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;




public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		
		Path arbres = new Path("/Users/andreasveen/Documents/workspace/WordCount/arbres.csv");
		//Path arbres = new Path(args[0]);
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(arbres);
		int antall = 0;
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			
			// read line by line
			String line = br.readLine();
			String annee = new String();
			String hauteur = new String();
			
			while (line !=null){
				antall += 1;
				annee = line.toString().split(";")[5];
				hauteur = line.toString().split(";")[6];
				// Process of the current line
				System.out.println(annee + "; " + hauteur);
				//System.out.println(line);
				// go to the next line
				line = br.readLine();
				
			}
		}
		
		finally{
			//close the file
			inStream.close();
			fs.close();
		}

		System.out.println(antall);
		
	}

}
