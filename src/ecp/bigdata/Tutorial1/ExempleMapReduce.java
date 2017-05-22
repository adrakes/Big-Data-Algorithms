package ecp.bigdata.Tutorial1;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;


public class ExempleMapReduce extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }


        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(getConf());

        job.setJobName("notre probleme exemple");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(ExempleMapReduce.class);

        job.setMapperClass(ExampleMap.class);

        job.setReducerClass(ExempleReduce.class);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(TypecleI.class);

        job.setMapOutputValueClass(TypevalI.class);


        job.setOutputKeyClass(TypeCleS.class);

        job.setOutputValueClass(TypeValS.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));

        FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));


        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {

            fs.delete(outputFilePath, true);

        }


        return job.waitForCompletion(true) ? 0: 1;

    }


    public static void main(String[] args) throws Exception {

        ExempleMapReduce exempleDriver = new ExempleMapReduce();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

