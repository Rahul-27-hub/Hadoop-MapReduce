import java.io.*;
import java.util.Scanner;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.w3c.dom.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class Project3 {
        public static class MapTitles extends Mapper<Object, Text, Text, Text> {
            @Override public void map(Object key, Text value, Context context){
                // This is our mapper 1 for titles 
                String line1 = value.toString();     // changing the text input from parameters to string
                String[] attributes = line1.split("  "); //splitting the line into multiple lines so we can store the values in diffrent array positions
                String t_id = attributes[0];    //so the title id will be stored as attirbute in position 0
                String t_type = attributes[1].toString();   // and the type will be stored as attribute in position 1 we change it to to string because it is text 
                String name = attributes[2];    // and so on
                String year = attributes[3];
                int y = year.parseInt();
                String genres = attributes[4];
                line1 = attributes[4].toString(); // same reason we change it to string is because it is text input
                ArrayList<String> genre = new ArrayList<String>();
                String[] genrelist = line1.split(",");
                
                for (int i=0;i<genrelist.length;i++){
                    genre.add(genrelist[i]);        // converting the genrelist to array to stroe the genre and add them together
                }
                String Input_line = t_type + ";" + name + ";" + year + ";" + genre;
                if(y>= 1957 && y<= 1967){
                if ((t_type.equals("movie") || t_type.equals("tvSeries") || t_type.equals("tvMovie")) && (genre.contains("Comedy") || genre.contains("Romance") || genre.contains("Action")))
                {
                    context.write(new Text(t_id), new Text("title   "+ Input_line));    // this will retrive all the title types equal to moveis or
                                                                                        // tvseries or tvmovies and check if genre contains comedy romance or action
                }
            }
        }
    }

                public static class MapActors extends Mapper<Object, Text, Text, Text> {
                    @Override
                    public void map ( Object key, Text value, Context context ){
                        // we do the same thing as we did in mapper 1 like convert the lines to string
                        // then we split the string so we can store multiple attirbutes in the same array and work on them
                        String line2 = value.toString();
                        String[] attirbutes1 = line2.split(";");
                        String id = attirbutes1[0];
                        String a_id = attirbutes1[1];
                        String a_name = attirbutes1[2];
                        String Input_line2 = a_id + ";" + a_name;
                        context.write(new Text(id), new Text("actor   "+ Input_line2));
                    }
                }

                public static class MapDirectors extends Mapper<Object, Text, Text, Text> {
                    @Override
                    public void map ( Object key, Text value, Context context )
                    throws IOException, InterruptedException {
                        // same for mapping directors 
                        String line3 = value.toString();
                        String[] attributes2 = line3.split(";");
                        String id = attributes2[0];
                        String d_id = attributes2[1];
                        context.write(new Text(id), new Text("director    " + d_id));
                    }
                }
                public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
                    @Override
                    public void reduce ( Text key, Iterable<Text> values, Context context )
                                       throws IOException, InterruptedException {
                                        String id = "";
                                        String name = "";
                                        String year = "";
                                        String type = "";
                                        String genre = "";
                                        ArrayList<String> actorIds = new ArrayList<String>();
                                        ArrayList<String> actorNames = new ArrayList<String>();
                                        ArrayList<String> directorIds = new ArrayList<String>();
                                        String line4 = "";
                                        int count = 0;
                                        boolean present = false;
                                        for (Text t: values){       // for every text input check the following
                                            line4 = t.toString();   // we will convert the text input to string input so we can work on it
                                            String[] parts = line4.split("   "); // so we split the lines in parts to check for constraints
                                            if(parts[0].equals("title")){ // first we will check if the  the parts equals title and if it does we take the type and name from it
                                                line4 = parts[1].toString();
                                                String[] attributes3 =line4.split(";");
                                                type = attributes3[0];
                                                name = attributes3[1];
                                                if(attributes3.length > 2){
                                                    year = attributes3[2];
                                                    if(attributes3.length > 3) {
                                                        genre = attributes3[3];
                                                    }

                                                }
                                                present = true;
                                            }
                                            else if (parts[0].equals ("actor")){ // if it doesnt equals title we check if it equals actor 
                                                line4 = parts[1].toString();
                                                String [] attributes3 = line4.split(";");
                                                actorIds.add(attributes3[0]); // now that we know it equals actor we compute the actor id and names
                                                actorNames.add(attributes3[1]);
                                            }
                                            else if(parts[0].equals("director")){
                                                // now we compute the same for director and see if parts equals director we get the values of director
                                                line4 = parts[1].toString();
                                                directorIds.add(line4);


                                            }
                                        }
                                        if(present) {
                                            if(actorIds.size() >0 && directorIds.size() > 0){
                                                for (int i=0; i<directorIds.size();i++){
                                                    if (actorIds.contains(directorIds.get(i))) {
                                                        int index = actorIds.indexOf(directorIds.get(i));
                                                        String actorName = actorNames.get(index);
                                                        String output = "    " + name + "    " + actorName + "    " + year + "    " + genre;
                                                        context.write(new Text(type),new Text(output));
                                                    }
                                                }
                                           }

                                        }

                                    }
    
                                
                            }
                        
                        public static void main(String[] args) throws Exception {
                            // read from 3 files and select movies where actor and director are same
                           Configuration conf = new Configuration();
                           Job job = Job.getInstance(conf, "actor-director gig");
                           conf.set("mapred.min.split.size","67108864");
                           conf.set("mapreduce.map.memory.mb","2048");
                           conf.set("mapreduce.reduce.memory.mb","2048");
                           
                           job.setJarByClass(Project3.class);
                           // config 1 with 3 mapper and 1 recder task
                           job.setNumReduceTasks(1);
                           MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapTitles.class);
                           MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapActors.class);
                           MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,MapDirectors.class);
                           
                           job.setReducerClass(Reducer1.class);
                           job.setMapOutputKeyClass(Text.class);
                           job.setMapOutputValueClass(Text.class);
                           job.setOutputKeyClass(Text.class);
                           job.setOutputValueClass(Text.class);
                           
                           FileOutputFormat.setOutputPath(job, new Path(args[3]+"inter"));
                           job.waitForCompletion(true);
                           System.exit(0);

                           // config 2 with 3 mappers and 2 reducer tasks
                           job.setNumReduceTasks(2);
                           MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapTitles.class);
                           MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapActors.class);
                           MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,MapDirectors.class);
                           
                           job.setReducerClass(Reducer1.class);
                           job.setMapOutputKeyClass(Text.class);
                           job.setMapOutputValueClass(Text.class);
                           job.setOutputKeyClass(Text.class);
                           job.setOutputValueClass(Text.class);
                           
                           FileOutputFormat.setOutputPath(job, new Path(args[3]+"inter"));
                           job.waitForCompletion(true);
                           System.exit(0);
                       }
}

