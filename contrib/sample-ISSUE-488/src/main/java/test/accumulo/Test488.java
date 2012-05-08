package test.accumulo;

import java.util.Collections;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test488 extends Configured implements Tool {
  
  private static Log LOGGER = LogFactory.getLog(Test488.class);
  
  public static class TestMapper extends Mapper<Key,Value,NullWritable,NullWritable> {
    @Override
    public void map(Key key, Value value, Context context) {
      LOGGER.info("Received a record. Key: " + key.getRow().toString() 
          + ", Status: " + context.getStatus() + ", Num records: " + context.getCounter(Counter.MAP_INPUT_RECORDS).getValue());
    }
  }
  
  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Test488(), args);
    System.exit(result);
  }
  
  public int run(String[] arg0) throws Exception {
    
    Integer timeoutVal = 11000;
    Job job = new Job(getConf());
    job.setInputFormatClass(AccumuloInputFormat.class);
    
    job.getConfiguration().set("mapreduce.task.timeout", "10000");
    job.getConfiguration().set("mapred.task.timeout", "10000");
    IteratorSetting sleepIterSettings = new IteratorSetting(Integer.MAX_VALUE, this.getClass().getName() + "-sleepIter", 
        SleepingIterator.class.getName(), Collections.singletonMap(SleepingIterator.KEY_SLEEP_DURATION_OPTION, 
            timeoutVal.toString()));
    AccumuloInputFormat.addIterator(job.getConfiguration(), sleepIterSettings);
    
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), "root", "cat".getBytes(), "test488", new Authorizations());
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), "test-1.4.0-development", "localhost");
    
    job.setMapperClass(TestMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setJobName("test-accumulo-488");
    job.setJarByClass(this.getClass());
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.waitForCompletion(true);
    return 0;
  }
}
