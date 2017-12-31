package org.atri.platodb.store.sequence;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * @author atri
 * @since 2017-mar-22 15:03:35
 */
public abstract class SequenceManager {

  public abstract void close() throws IOException;

  public abstract SequenceManager.Sequence.ReservedSequenceRange reserve(String name, int requestedSize) throws IOException;
  public abstract <T> Sequence<T> getOrRegisterSequence(Class<T> valueType, String name) throws IOException;

  public abstract <T> Sequence<T> sequenceFactory(Class<T> valueType, String name) throws IOException;

  public abstract class Sequence<T> {

    public abstract void close() throws IOException;

    private String name;

    protected Sequence(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public T next() throws IOException {
      return reserve(1).nextValue();
    }

    public abstract Class<T> getValueType();
    public abstract ReservedSequenceRange<T> reserve(int requestedSize) throws IOException;

    /**
     * if applicable to sequence manager implementation,
     * then the unused reserved values are queued up to get reused.
     * @param reservation 
     */
    protected abstract void release(ReservedSequenceRange<T> reservation);

    public abstract class ReservedSequenceRange<T> {
      public abstract int size();
      public abstract T nextValue() throws NoSuchElementException;
      public abstract boolean hasNextValue();
    }

  }
}
