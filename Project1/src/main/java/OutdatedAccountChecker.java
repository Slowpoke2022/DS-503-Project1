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

public class OutdatedAccountChecker {

    public static class AccessLogsMapper1
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String pageID = fields[1];
            Integer access = Integer.parseInt(fields[4]);
            access = access / (24 * 60);
            context.write(new Text(pageID), new Text("A" + "," + access));
        }
    }

    public static class AccessLogsMapper2
            extends Mapper<Object, Text, Text, Text>{
        private Map<String, Integer> accessMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            accessMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String pageID = fields[1];
            Integer access = Integer.parseInt(fields[4]);
            access = access / (24 * 60);
            Integer current = accessMap.get(pageID);
            if (current == null || current > access) {
                accessMap.put(pageID, access);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String pageID : accessMap.keySet()) {
                Integer last_access = accessMap.get(pageID);
                context.write(new Text(pageID), new Text("A" + "," + last_access.toString()));
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

        private Map<String, Integer> accessMap;
        private Map<String, String> dataMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            accessMap = new HashMap<>();
            dataMap = new HashMap<>();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            Integer access = Integer.MAX_VALUE;
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    access = Math.min(Integer.parseInt(parts[1]), access);
                }
                else if (parts[0].equals("F")) {
                    name = parts[1];
                }
            }
            accessMap.put(key.toString(), access);
            dataMap.put(key.toString(), name);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String pageID : accessMap.keySet()) {
                String name = dataMap.get(pageID);
                Integer last_access = accessMap.get(pageID);
                if (last_access > 90) {
                    context.write(new Text(pageID), new Text(name + " " + last_access));
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "outdated checker");
        job.setJarByClass(OutdatedAccountChecker.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "outdated checker");
        job.setJarByClass(OutdatedAccountChecker.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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
}
