package ecp.bigdata.Tutorial1;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.io.Iterable;

// A compléter selon le problème

public class ExempleReduce extends Reducer<TypeCleI,TypeValI,TypeCleS,TypeValS> {

    // Écriture de la fonction reduce

    @Override

    protected void reduce(TypeCleI cleI, Iterable<TypeValI> listevalI, Context context) throws IOException,InterruptedException

    {

        // À compléter selon le probleme

        TypeCleS cleS = new TypeCleS(...);

        TypeValS valS = new TypeValS(...);

        for (TypeValI val: listevalI) {

            // traitement cleS.set(...), valS.set(...)

        }

        context.write(cleS,valS);

    }

}

