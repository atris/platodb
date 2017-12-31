package org.atri.platodb.store.sequence;

import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;

/**
 * identities based on system time
 *
 * @author atri
 * @since 2017-mar-23 01:41:16
 */
public class UnsafeSequenceManager extends SequenceManager {

  private Map<String, Sequence> sequences = new HashMap<String, Sequence>();

  public void close() throws IOException {
    for (Sequence sequence : sequences.values()) {
      sequence.close();
    }
  }

  public SequenceManager.Sequence.ReservedSequenceRange reserve(String name, int requestedSize) throws IOException {
    return sequences.get(name).reserve(requestedSize);
  }

  @SuppressWarnings("unchecked")
  public <T> Sequence<T> getOrRegisterSequence(Class<T> valueType, String name) {
    Sequence<T> sequence = sequences.get(name);
    if (sequence == null) {
      sequences.put(name, sequence = sequenceFactory(valueType, name));
    }
    return sequence;
  }

  @SuppressWarnings("unchecked")
  public <T> Sequence<T> sequenceFactory(Class<T> valueType, String name) {
    if (valueType == Long.class) {
      return (Sequence<T>) new LongSequence(name);
    }
    throw new UnsupportedOperationException("Unregistred value type class " + valueType.getName());
  }


  public class LongSequence extends Sequence<Long> {

    public void close() throws IOException {
      
    }

    public LongSequence(String name) {
      super(name);
    }

    public Class<Long> getValueType() {
      return Long.class;
    }

    public synchronized ReservedSequenceRange<Long> reserve(int requestedSize) {

      long start = System.currentTimeMillis();

      try {
        Thread.sleep(requestedSize + 5);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }

      return new LongRange(start, start + requestedSize);
    }

    protected void release(ReservedSequenceRange<Long> reservation) {
      // ignored
    }

    public class LongRange extends ReservedSequenceRange<Long> {

      private long start;
      private long end;
      private long size;
      private AtomicLong current;

      protected LongRange(long start, long end) {
        this.start = start;
        this.end = end;
        size = end - start;
        current = new AtomicLong(start);
      }

      public boolean hasNextValue() {
        return current.get() < end;
      }

      public Long nextValue() {
        if (!hasNextValue()) {
          throw new NoSuchElementException();
        }
        return current.incrementAndGet();
      }

      public int size() {
        return (int) size;
      }

      @Override
      public String toString() {
        return "LongRange{" +
            "start=" + start +
            ", end=" + end +
            ", size=" + size +
            ", current=" + current +
            '}';
      }
    }

  }
}




