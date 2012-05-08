package test.accumulo;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class SleepingIterator extends WrappingIterator implements OptionDescriber {
  
  public static final String KEY_SLEEP_DURATION_OPTION = "sleepDuration";
  private int sleepDuration = 0;
  
  @Override
  public void next() throws IOException {
    try {
      Thread.sleep(sleepDuration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    super.next();
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    
    String sleepDurationString = options.get(KEY_SLEEP_DURATION_OPTION);
    if (sleepDurationString != null)
      this.sleepDuration = Integer.parseInt(sleepDurationString);
  }
  
  public static void setSleepDuration(IteratorSetting cfg, int numMilliSeconds) {
    cfg.addOption(KEY_SLEEP_DURATION_OPTION, Integer.toString(numMilliSeconds));
  }
  
  public IteratorOptions describeOptions() {
    return new IteratorOptions(KEY_SLEEP_DURATION_OPTION, "Milliseconds to sleep before reading a record", 
        Collections.singletonMap(KEY_SLEEP_DURATION_OPTION, "amount of time (milliseconds) to sleep when reading a record"), null);
  }
  
  public boolean validateOptions(Map<String,String> options) {
    int i = Integer.parseInt(options.get(KEY_SLEEP_DURATION_OPTION));
    if (i < 0)
      throw new IllegalArgumentException(KEY_SLEEP_DURATION_OPTION + " for sleep iterator must be >= 0");
    return true;
  }
  
}
