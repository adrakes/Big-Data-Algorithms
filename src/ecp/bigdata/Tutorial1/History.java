package ecp.bigdata.Tutorial1;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class History {

	public static void main(String[] args) throws IOException {
		
		
		Path history = new Path("/Users/andreasveen/Documents/workspace/WordCount/isd-history.txt");
		//Path history = new Path(args[0]);
		
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(history);
		int antall = 0;
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			
			// read line by line
			
			String line = br.readLine();
			String USAF = new String();
			String name = new String();
			String FIPS = new String();
			String altitude = new String();
			
			while (line  !=null){
				antall += 1;
				line = br.readLine();
				//System.out.println(antall);
			
			
			if (line !=null && antall >= 22){
				USAF = line.substring(0,6);
				name = line.substring(13,(29+13));
				FIPS = line.substring(43,43+2);
				altitude = line.substring(74,74+7);
				//annee = line.toString().split(";")[5];
				//hauteur = line.toString().split(";")[6];
				// Process of the current line
				//System.out.println(annee + "; " + hauteur);
				//System.out.println(line);
				//System.out.println(line.length());
				//System.out.println(antall);
				System.out.println(USAF + "; " + name + "; " + FIPS + "; " + altitude);
				// go to the next line
				line = br.readLine();
				
			}
		}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}

		//System.out.println(antall);
		
	}

}
