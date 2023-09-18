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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class TopTenUsers {

    public static class TopTenMapper
            extends Mapper<Object, Text, Text, Text>{
        private Map<String, String[]> dataMap;
        private Map<String, Integer> countMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            dataMap = new HashMap<>();
            countMap = new HashMap<>();
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] data = new String[2];
                String[] items = line.split(",");
                data[0] = items[1];
                data[1] = items[2];
                dataMap.put(items[0], data);
            }
            IOUtils.closeStream(reader);
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
                context.write(new Text(pageID), new Text(count.toString() + "," + data[0] + "," + data[1]));
            }
        }
    }

    public static class TopTenReducer
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
            for (Text value : values) {
                String[] items = value.toString().split(",");
                String[] data = new String[2];
                data[0] = items[1];
                data[1] = items[2];
                countMap.put(key.toString(), Integer.parseInt(items[0]));
                dataMap.put(key.toString(), data);
            }
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
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/FaceInPage.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
        job.setJarByClass(TopTenUsers.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///D:/DS503/FaceInPage.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}