import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class NoInteractionFriends {

    public static class AssociatesMapper
            extends Mapper<Object, Text, Text, Text>{
        private Map<ArrayList<String>, Integer> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            countMap = new HashMap<>();
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] items = line.split(",");
                ArrayList<String> pair = new ArrayList<>(Arrays.asList(items[1], items[2]));
                Integer count = countMap.get(pair);
                if (count == null) {
                    countMap.put(pair, 1);
                } else {
                    countMap.put(pair, count + 1);
                }
            }
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String personA = fields[1];
            String personB = fields[2];
            ArrayList<String> access = new ArrayList<>(Arrays.asList(personA, personB));
            Integer count = countMap.get(access);
            if (count != null) {
                context.write(new Text(personA), new Text("A" + "," + personB + "," + "1"));
            } else {
                context.write(new Text(personA), new Text("A" + "," + personB + "," + "0"));
            }
            access = new ArrayList<>(Arrays.asList(personB, personA));
            count = countMap.get(access);
            if (count != null) {
                context.write(new Text(personB), new Text("A" + "," + personA + "," + "1"));
            } else {
                context.write(new Text(personB), new Text("A" + "," + personA + "," + "0"));
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

        private Map<ArrayList<String>, Integer> countMap;
        private Map<String, String> dataMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            countMap = new HashMap<>();
            dataMap = new HashMap<>();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    String accessed = parts[1];
                    Integer count = Integer.parseInt(parts[2]);
                    ArrayList<String> pair = new ArrayList<>(Arrays.asList(key.toString(), accessed));
                    Integer total = countMap.get(pair);
                    if (total == null) {
                        countMap.put(pair, count);
                    } else {
                        countMap.put(pair, total + count);
                    }
                }
                else if (parts[0].equals("F")) {
                    dataMap.put(key.toString(), parts[1]);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (ArrayList<String> pair : countMap.keySet()) {
                Integer count = countMap.get(pair);
                if (count == 0) {
                    String pageID = pair.get(0);
                    String name = dataMap.get(pageID);
                    context.write(new Text(pageID), new Text(name));
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "no interact");
        job.setJarByClass(NoInteractionFriends.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/AccessLogs.csv"));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "no interact");
        job.setJarByClass(NoInteractionFriends.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/AccessLogs.csv"));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }
}
