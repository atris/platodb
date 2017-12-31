package org.atri.platodb.store.sequence;

import org.atri.platodb.store.lock.Lock;
import org.atri.platodb.store.lock.LockFactory;

import java.util.NoSuchElementException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;

/**
 * @author atri
 * @since 2017-mar-23 16:39:45
 */
public class FilebasedSequenceManager extends SequenceManager {

  private File path;
  private LockFactory lockFactory;
  private long lockWaitTimeout;

  private Map<String, Sequence> sequences = new HashMap<String, Sequence>();

  public void close() throws IOException {
    for (Sequence sequence : sequences.values()) {
      sequence.close();
    }
  }

  public FilebasedSequenceManager(File path, LockFactory lockFactory, long lockWaitTimeout) throws IOException {
    this.path = path;
    this.lockFactory = lockFactory;
    this.lockWaitTimeout = lockWaitTimeout;
    if (!path.exists()) {
      if (!path.mkdirs()) {
        throw new IOException("Could not create path " + path.getAbsolutePath());
      }
    }
  }

  public Sequence.ReservedSequenceRange reserve(String name, int requestedSize) throws IOException {
    return sequences.get(name).reserve(requestedSize);
  }

  public <T> Sequence<T> getOrRegisterSequence(Class<T> valueType, String name) throws IOException {
    Sequence<T> sequence = sequences.get(name);
    if (sequence == null) {
      synchronized (this) {
        sequence = sequences.get(name);
        if (sequence == null) {
          sequence = sequenceFactory(valueType, name);
          sequences.put(name, sequence);
        }
      }
    }
    return sequence;
  }

  public <T> Sequence<T> sequenceFactory(Class<T> valueType, String name) throws IOException {
    if (Long.class == valueType) {
      return (Sequence<T>) new LongSequence(name);
    } else {
      throw new UnsupportedOperationException("Can not handle type " + valueType.getName() + " for sequence named " + name);
    }
  }

  public abstract class Sequence<T> extends SequenceManager.Sequence<T> {


    protected Sequence(String name) {
      super(name);
    }

    protected final void release(ReservedSequenceRange<T> reservation) {
      // ignored  todo
    }


  }

  public abstract class NumbericSequence<T> extends Sequence<T> {

    private File file;
    protected final RandomAccessFile RAF;
    protected final Lock lock;

    protected NumbericSequence(String name) throws IOException {
      super(name);
      file = new File(path, name + ".seq");
      lock = lockFactory.makeLock(file.getName());
      boolean create = !file.exists();

      if (create) {
        RAF = new Lock.With<RandomAccessFile>(lock, lockWaitTimeout) {
          public RandomAccessFile doBody() throws IOException {
            if (!file.exists()) {
              file.createNewFile();
              RandomAccessFile RAF = new RandomAccessFile(file, "rw");
              initializeHeader(RAF);
              return RAF;
            } else {
              return new RandomAccessFile(file, "rw");
            }
          }
        }.run();
      } else {
        RAF = new RandomAccessFile(file, "rw");
      }
    }

    public void close() throws IOException {
      RAF.close();
    }

    protected abstract void initializeHeader(RandomAccessFile headerRAF) throws IOException;

    public abstract class NumericRange<T> extends ReservedSequenceRange<T> {
      private final int size;
      protected final T start;
      protected final T end;

      protected NumericRange(int size, T start, T end) {
        this.size = size;
        this.start = start;
        this.end = end;
      }

      public int size() {
        return size;
      }
    }
  }

  public class LongSequence extends FilebasedSequenceManager.NumbericSequence<Long> {

    public LongSequence(String name) throws IOException {
      super(name);
    }

    protected void initializeHeader(RandomAccessFile headerRAF) throws IOException {
      headerRAF.seek(0);
      headerRAF.writeLong(1l);
    }

    public Class<Long> getValueType() {
      return Long.class;
    }

    public ReservedSequenceRange<Long> reserve(final int requestedSize) throws IOException {
      long start = new Lock.With<Long>(lock, lockWaitTimeout) {
        public Long doBody() throws IOException {
          RAF.seek(0);
          long next = RAF.readLong();
          RAF.seek(0);
          RAF.writeLong(next + requestedSize);
          return next;
        }
      }.run();

      return this.new LongRange(start, start + requestedSize);
    }


    public class LongRange extends NumericRange<Long> {

      private AtomicLong nextValue;

      protected LongRange(long start, long end) {
        super((int) (end - start), start, end);
        nextValue = new AtomicLong(start);
      }

      public synchronized Long nextValue() throws NoSuchElementException {
        if (!hasNextValue()) {
          throw new NoSuchElementException();
        }
        return nextValue.getAndIncrement();
      }

      public boolean hasNextValue() {
        return nextValue.get() <= end;
      }
    }

  }


}
