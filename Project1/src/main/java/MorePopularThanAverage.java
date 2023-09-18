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

public class MorePopularThanAverage {

    public static class AssociatesMapper
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

        double total;
        int n;
        private Map<String, String> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            total = 0.0;
            n = 0;
            countMap = new HashMap<>();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            int count = 0;
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("A")) {
                    count += Integer.parseInt(parts[1]);
                    total += count;
                }
                else if (parts[0].equals("F")) {
                    name = parts[1];
                }
            }
            n += 1;
            countMap.put(key.toString(), name + "," + Integer.toString(count));
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            double average = total / n;
            String average_val = String.format("%f", average);
            context.write(new Text("average"), new Text(average_val));
            for (String pageID : countMap.keySet()) {
                String[] data = countMap.get(pageID).split(",");
                if (Double.parseDouble(data[1]) > average) {
                    context.write(new Text(pageID), new Text(data[0] + " " + data[1]));
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "more popular");
        job.setJarByClass(MorePopularThanAverage.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "more popular");
        job.setJarByClass(MorePopularThanAverage.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}