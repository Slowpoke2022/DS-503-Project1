import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HaveFavoriteChecker {


    public static class AccessLogsMapper1
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String accessor = fields[1];
            String accessed = fields[2];
            context.write(new Text(accessor), new Text("A" + "," + "1" + "," + accessed));
        }

    }

    public static class AccessLogsMapper2
            extends Mapper<Object, Text, Text, Text>{
        private Map<String, Integer> countMap;
        private Map<String, HashSet<String>> idMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            countMap = new HashMap<>();
            idMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String accessor = fields[1];
            String accessed = fields[2];
            Integer count = countMap.get(accessor);
            if (count == null) {
                countMap.put(accessor, 1);
            } else {
                countMap.put(accessor, count + 1);
            }
            HashSet<String> uniqueIDs = idMap.get(accessor);
            if (uniqueIDs == null) {
                HashSet<String> temp = new HashSet<>();
                temp.add(accessed);
                idMap.put(accessor, temp);
            } else {
                uniqueIDs.add(accessed);
                idMap.put(accessor, uniqueIDs);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String pageID : countMap.keySet()) {
                Integer total = countMap.get(pageID);
                String unique = "";
                Object[] temp = idMap.get(pageID).toArray();
                for (int i = 0; i < temp.length; i++) {
                    if (i < temp.length - 1) {
                        unique += temp[i] + ",";
                    } else {
                        unique += temp[i];
                    }
                }
                context.write(new Text(pageID), new Text("A" + "," + total.toString() + "," + unique));
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
            HashSet<String> unique = new HashSet<>();
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    count += Integer.parseInt(parts[1]);
                    if (parts.length == 2) {
                        unique.add(parts[2]);
                    } else {
                        for (int i = 2; i < parts.length; i++) {
                            unique.add(parts[i]);
                        }
                    }
                }
                else if (parts[0].equals("F")) {
                    name = parts[1];
                }
            }
            if (count != unique.size()) {
                context.write(key, new Text(name + " " + Integer.toString(count) + " " + Integer.toString(unique.size())));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorite checker");
        job.setJarByClass(HaveFavoriteChecker.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        if (args[3].equals("optimized")) {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper2.class);
        } else {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper1.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorite checker");
        job.setJarByClass(HaveFavoriteChecker.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        if (args[3].equals("optimized")) {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper2.class);
        } else {
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper1.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}