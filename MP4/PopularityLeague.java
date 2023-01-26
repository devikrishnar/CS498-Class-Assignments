import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tempPath = new Path("./tmp");
        fs.delete(tempPath, true);

        Job linkCountJob = Job.getInstance(conf, "link_count");
        linkCountJob.setOutputKeyClass(IntWritable.class);
        linkCountJob.setOutputValueClass(IntWritable.class);

        linkCountJob.setMapperClass(LinkCountMap.class);
        linkCountJob.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(linkCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(linkCountJob, tempPath);

        linkCountJob.setJarByClass(PopularityLeague.class);
        linkCountJob.waitForCompletion(true);

        Job popularityLeague = Job.getInstance(conf, "popularity_league");
        popularityLeague.setOutputKeyClass(NullWritable.class);
        popularityLeague.setOutputValueClass(IntArrayWritable.class);

        popularityLeague.setInputFormatClass(KeyValueTextInputFormat.class);
        
        popularityLeague.setOutputFormatClass(TextOutputFormat.class);

        popularityLeague.setMapperClass(LinkRankMap.class);
        popularityLeague.setReducerClass(LinkRankReduce.class);

        FileInputFormat.setInputPaths(popularityLeague, tempPath);
        FileOutputFormat.setOutputPath(popularityLeague, new Path(args[1]));

        popularityLeague.setJarByClass(PopularityLeague.class);
        return popularityLeague.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            IntWritable page = new IntWritable(Integer.parseInt(tokenizer.nextToken().trim()));
            context.write(page, new IntWritable(0));

            while (tokenizer.hasMoreTokens()) {
                String linkedPage = tokenizer.nextToken().trim();
                context.write(new IntWritable(Integer.parseInt(linkedPage)), new IntWritable(1));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static class LinkRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        TreeSet<Pair<Integer, Integer>> countToLinksMap = new TreeSet<Pair<Integer, Integer>>();
        List<Integer> leagueList = new ArrayList<Integer>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
  
            Path leaguePath = new Path(conf.get("league"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(leaguePath))
            );
            String line = reader.readLine();

            while (line != null) {
                leagueList.add(Integer.parseInt(line.trim()));
                line = reader.readLine();
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            Integer link = Integer.parseInt(key.toString());

            if (leagueList.contains(link)) {
		        countToLinksMap.add(new Pair<Integer, Integer>(link, count));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : countToLinksMap) {
                Integer[] integers = {item.first, item.second};
                IntArrayWritable value = new IntArrayWritable(integers);
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static class LinkRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        TreeSet<Pair<Integer, Integer>> countToLinksMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable value : values) {
                IntWritable[] pair = (IntWritable[]) value.toArray();
                countToLinksMap.add(new Pair<Integer, Integer>(pair[0].get(), pair[1].get()));
            }

            TreeSet<Pair<Integer, Integer>> countToLinksMap_desc = (TreeSet<Pair<Integer, Integer>>)countToLinksMap.descendingSet();

            for (Pair<Integer, Integer> item : countToLinksMap_desc) {
                int rank = 0;

                for (Pair<Integer, Integer> otherItem : countToLinksMap_desc) {
                    if (item.second.compareTo(otherItem.second) > 0) {
                        rank++;		
                    }
                }
                
                context.write(new IntWritable(item.first), new IntWritable(rank));
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
