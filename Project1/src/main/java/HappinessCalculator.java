import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HappinessCalculator {


    public static class AssociatesMapper1
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String personA = fields[1];
            String personB = fields[2];
            context.write(new Text(personA), new Text("A" + "," + "1"));
            context.write(new Text(personB), new Text("A" + "," + "1"));
        }
    }

    public static class AssociatesMapper2
            extends Mapper<Object, Text, Text, Text>{
        private Map<String, Integer> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            countMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String personA = fields[1];
            String personB = fields[2];
            Integer countA = countMap.get(personA);
            if (countA == null) {
                countMap.put(personA, 1);
            } else {
                countMap.put(personA, countA + 1);
            }
            Integer countB = countMap.get(personB);
            if (countB == null) {
                countMap.put(personB, 1);
            } else {
                countMap.put(personB, countB + 1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String pageID : countMap.keySet()) {
                String count = countMap.get(pageID).toString();
                context.write(new Text(pageID), new Text("A" + "," + count));
            }
        }
    }

    public static class FaceInPageMapper
            extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String pageID = fields[0];
            String person = fields[1];
            context.write(new Text(pageID), new Text("F" + "," + person));
        }
    }

    public static class ReduceJoinReducer
            extends Reducer <Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            int count = 0;
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    count += Integer.parseInt(parts[1]);
                }
                else if (parts[0].equals("F")) {
                    name = parts[1];
                }
            }
            context.write(key, new Text(name + " " + Integer.toString(count)));
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "happiness calculator");
        job.setJarByClass(HappinessCalculator.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        if (args[3].equals("optimized")) {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper2.class);
        } else {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper1.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "happiness calculator");
        job.setJarByClass(HappinessCalculator.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        if (args[3].equals("optimized")) {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper2.class);
        } else {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper1.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }
}
