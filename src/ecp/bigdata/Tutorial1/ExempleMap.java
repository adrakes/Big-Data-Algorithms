package ecp.bigdata.Tutorial1;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// A compléter selon le problème
public class ExempleMap extends Mapper<TypeCleE, TypeValE, TypeCleI, TypeValI> {
// Écriture de la fonction map
@Override
protected void map(TypeCleE cleE, TypeValE valE, Context context) throws IOException,InterruptedException
    {
        // À compléter selon le probleme
        // traitement : cleI = ..., valI = ...
        TypeCleI cleI = new TypeCleI(...);
        TypeValI valI = new TypeValI(...);
        context.write(cleI,valI);
    }
}






