package org.atri.platodb;

import org.atri.platodb.store.Accessor;
import org.atri.platodb.store.Log;
import org.atri.platodb.store.Store;
import org.atri.platodb.store.lock.Lock;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author atri
 * @since 2017-mar-17 11:36:10
 */
public class Benchmark {

  private static final Log log = new Log(Benchmark.class);

  public static void main(String[] arg) throws Exception {

    log.info("Arguments: [min value posting byte[] size] [max value posting byte[] size] [number of postings]  [debug interval]");

    Benchmark benchmark = new Benchmark();

    if (arg.length == 4) {
      benchmark.benchmarkStore(Integer.valueOf(arg[0]), Integer.valueOf(arg[1]), Integer.valueOf(arg[2]), Integer.valueOf(arg[3]));
    } else {
      benchmark.benchmarkStore(1, 10000, 1000000, 10000);
    }

  }

  private Random random;

  public Benchmark() {
    long seed = System.currentTimeMillis();
    log.info("Random seed: " + seed);
    random = new Random(seed);

  }

  private DecimalFormat df = new DecimalFormat("0.000");


  private void benchmarkStore(int minValueSize, int maxValueSize, final int benchmarkSize, final int debugInterval) throws IOException {

    log.info("Benchmarking store. minValueSize=" + minValueSize + ", maxValueSize=" + maxValueSize + ", benchmarkSize=" + benchmarkSize + ", debugInterval=" + debugInterval);

    final byte[][] values = createBuffers(minValueSize, maxValueSize);
    final Set<Long> keySet = new HashSet<Long>(benchmarkSize * 2);
    final List<Long> keyList = new ArrayList<Long>(benchmarkSize * 2);

    for (int i = 0; i < benchmarkSize; i++) {
      long key;
      while (!keySet.add(key = random.nextLong())) ;
      keyList.add(key);
    }

    final Store store = new Store(new File("benchmark/" + System.currentTimeMillis()));

    final Accessor accessor = store.borrowAccessor();

    final long revision = accessor.increaseStoreRevision();

    log.info("Starting benchmark..");

    final long writeStart = System.currentTimeMillis();

    Lock.With with = new Lock.With(accessor.getStoreWriteLock(), 0) {
      public Object doBody() throws IOException {

        int replaced = 0;

        long readStartLast = System.currentTimeMillis();

        LinkedList<Long> keys = new LinkedList<Long>(keyList);

        int cnt = 0;
        for (int i = 0; i < benchmarkSize; i++) {
          long key = keys.removeFirst();

          byte[] value = values[random.nextInt(values.length)];
          random.nextBytes(value);

          if (store.put(accessor, toByteArray(key), key, value, revision) != null) {
            replaced++;
          }

          if (cnt++ == debugInterval) {
            long time = System.currentTimeMillis() - writeStart;
            long timeLast = System.currentTimeMillis() - readStartLast;

            log.info("Wrote " + i + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) i)) + " ms/posting. " + df.format(((double) timeLast / (double) debugInterval)) + " ms/posting since the last report.");
            cnt = 1;
            readStartLast = System.currentTimeMillis();

          }

        }

        return null;
      }
    };
    with.run();


    long time = System.currentTimeMillis() - writeStart;
    log.info("Wrote " + benchmarkSize + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) benchmarkSize)) + " ms/posting. ");


    {
      ArrayList<Long> keys = new ArrayList<Long>(keyList);
      ArrayList<Long> randomOrderedKeys = new ArrayList<Long>(keyList.size());
      for (int i = 0; i < keyList.size(); i++) {
        randomOrderedKeys.add(keys.remove(random.nextInt(keys.size())));
      }

      log.info("Begin retrieving postings in random order");

      long readStart = System.currentTimeMillis();
      long readStartLast = System.currentTimeMillis();
      int cnt = 0;
      for (int i = 0; i < randomOrderedKeys.size(); i++) {
        long key = randomOrderedKeys.get(i);
        store.get(accessor, toByteArray(key), key);
        if (cnt++ == debugInterval) {
          time = System.currentTimeMillis() - readStart;
          long timeLast = System.currentTimeMillis() - readStartLast;
          log.info("Retrieved (random order) " + i + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) i)) + " ms/posting. " + df.format(((double) timeLast / (double) debugInterval)) + " ms/posting since the last report.");
          cnt = 1;
          readStartLast = System.currentTimeMillis();
        }
      }

      long timeLast = System.currentTimeMillis() - readStartLast;

      log.info("Retrieved (random order) " + benchmarkSize + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) benchmarkSize)) + " ms/posting. " + df.format(((double) timeLast / (double) debugInterval)) + " ms/posting since the last report.");


    }


    {
      ArrayList<Long> reversedOrderedKeys = new ArrayList<Long>(keyList.size());
      for (long key : keyList) {
        reversedOrderedKeys.add(0, key);
      }

      log.info("Begin retrieving postings in inverse chronological order (i.e. maximum number of seeks in order to get a post first)");

      long readStart = System.currentTimeMillis();
      long readStartLast = System.currentTimeMillis();
      int cnt = 0;
      for (int i = 0; i < benchmarkSize; i++) {
        long key = reversedOrderedKeys.get(i);
        store.get(accessor, toByteArray(key), key);
        if (cnt++ == debugInterval) {
          time = System.currentTimeMillis() - readStart;
          long timeLast = System.currentTimeMillis() - readStartLast;
          log.info("Retrieved (inverse chronologial order) " + i + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) i)) + " ms/posting. " + df.format(((double) timeLast / (double) debugInterval)) + " ms/posting since the last report.");
          cnt = 1;
          readStartLast = System.currentTimeMillis();
        }
      }

      long timeLast = System.currentTimeMillis() - readStartLast;

      log.info("Retreived (inverse chronologial order) " + benchmarkSize + " postings in " + (time / 1000) + " seconds or " + df.format(((double) time / (double) benchmarkSize)) + " ms/posting. " + df.format(((double) timeLast / (double) debugInterval)) + " ms/posting since the last report.");


    }

    {
      log.info("Rehashing with factor 1.7");
      long start = System.currentTimeMillis();
      store.rehash(accessor, (int) (store.getConfiguration().getInitialCapacity() * 1.7f));
      time = System.currentTimeMillis() - start;
      log.info("Rehashing took " + (time / 1000) + " seconds");
    }

    store.returnAccessor(accessor);

    store.close();
  }


  private byte[][] createBuffers(int minSize, int maxSize) {
    byte[][] buffers = new byte[maxSize - minSize][];
    int index = 0;
    for (int size = minSize; size < maxSize; size++) {
      buffers[index++] = new byte[size];
    }
    return buffers;
  }

  public static byte[] toByteArray(long l) {
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      b[3 - i] = (byte) (l >>> (i * 8));
    }
    return b;
  }

  public static long toLong(byte[] b) {
    long l = 0;
    for (int i = 0; i < 4; i++) {
      l <<= 8;
      l ^= (long) b[i] & 0xFF;
    }
    return l;
  }

}
