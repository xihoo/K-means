import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduce {
    
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{

        
        ArrayList<ArrayList<Double>> centers = null;
        
        int k = 0;
        
        
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            k = centers.size();
        }


        
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();
            
            double minDistance = 99999999;
            int centerIndex = 0;
            
            
            for(int i=0;i<k;i++){
                double currentDistance = 0;
                for(int j=0;j<sizeOfFileds;j++){
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double filed = Math.abs(fileds.get(j));
                    currentDistance += Math.pow((centerPoint - filed) / (centerPoint + filed), 2);
                }
                
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
           
            context.write(new IntWritable(centerIndex+1), value);
        }
        
    }
    
    
    public static class Reduce extends Reducer<IntWritable, Text, Text, Text>{

        
        
        protected void reduce(IntWritable key, Iterable<Text> value,Context context)
                throws IOException, InterruptedException {
            ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
            
           
            for(Iterator<Text> it =value.iterator();it.hasNext();){
                ArrayList<Double> tempList = Utils.textToArray(it.next());
                filedsList.add(tempList);
            }
            
           
            int filedSize = filedsList.get(0).size();
            double[] avg = new double[filedSize];
            for(int i=0;i<filedSize;i++){
                double sum = 0;
                int size = filedsList.size();
                for(int j=0;j<size;j++){
                    sum += filedsList.get(j).get(i);
                }
                avg[i] = sum / size;
            }
            context.write(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
        }
        
    }
    
    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{
        
        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);
        
        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(MapReduce.class);
        
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(runReduce){
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
        
        FileInputFormat.addInputPath(job, new Path(dataPath));
        
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));
        
        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String centerPath = "hdfs://localhost:9000/input/centers.txt";
        String dataPath = "hdfs://localhost:9000/input/wine.txt";
        String newCenterPath = "hdfs://localhost:9000/out/kmean";
        
        int count = 0;
        
        
        while(true){
            run(centerPath,dataPath,newCenterPath,true);
            System.out.println(" 第 " + ++count + " 次计算 ");
            if(Utils.compareCenters(centerPath,newCenterPath )){
                run(centerPath,dataPath,newCenterPath,false);
                break;
            }
        }
    }
    
}