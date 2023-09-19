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

public class TopTenUsers {

    public static class AccessLogsMapper
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
            String pageID = fields[2];
            Integer count = countMap.get(pageID);
            if (count == null) {
                countMap.put(pageID, 1);
            } else {
                countMap.put(pageID, count + 1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String pageID : countMap.keySet()) {
                Integer count = countMap.get(pageID);
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
            String nationality = fields[2];
            context.write(new Text(pageID), new Text("F" + "," + person + "," + nationality));
        }
    }

    public static class ReduceJoinReducer
            extends Reducer<Text, Text, Text, Text> {
        private Map<String, String[]> dataMap;
        private Map<String, Integer> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            dataMap = new HashMap<>();
            countMap = new HashMap<>();
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer count = 0;
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    count += Integer.parseInt(parts[1]);
                }
                else if (parts[0].equals("F")) {
                    String[] data = new String[2];
                    data[0] = parts[1];
                    data[1] = parts[2];
                    dataMap.put(key.toString(), data);
                }
            }
            countMap.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            List<String> pageIDs = new ArrayList<>(countMap.keySet());
            List<Integer> counts = new ArrayList<>(countMap.values());
            List<Integer> sorted = new ArrayList<>(counts);
            Collections.sort(sorted, Collections.reverseOrder());
            for (int i = 0; i < 10; i++) {
                Integer count = sorted.get(i);
                int index = counts.indexOf(count);
                String pageID = pageIDs.get(index);
                String[] data = dataMap.get(pageID);
                counts.remove(index);
                pageIDs.remove(index);
                context.write(new Text(pageID), new Text(count.toString() + " " + data[0] + " " + data[1]));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
        job.setJarByClass(TopTenUsers.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
        job.setJarByClass(TopTenUsers.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}