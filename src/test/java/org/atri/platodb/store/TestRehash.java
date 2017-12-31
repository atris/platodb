package org.atri.platodb.store;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author atri
 * @since 2017-mar-17 14:50:11
 */
public class TestRehash extends StoreTest {


  @Test
  public void testSimple() throws IOException {

    Configuration configuration = new Configuration(getDirectory("rehash"));
    configuration.setInitialCapacity(20);
    Store store = new Store(configuration);
    store.open();

    Accessor accessor = store.borrowAccessor();

    assertEquals(0, accessor.getHashtable().getVersionId());

    Random random = new Random(0);

    byte[][] values = new byte[256][];
    byte[][] keys = new byte[256][];
    long[] hashCodes = new long[256];
    float hashCode = 0f;

    for (int i = 0; i < 256; i++) {
      keys[i] = new byte[random.nextInt(100)+1];
      random.nextBytes(keys[i]);
      values[i] = new byte[random.nextInt(5000)+1];
      random.nextBytes(values[i]);
      hashCode += 0.3f;
      hashCodes[i] = (long)hashCode;

      store.put(accessor, keys[i], hashCodes[i], values[i], 0l);
    }

    for (int i=0; i< 256; i++) {
      byte[] value = store.get(accessor, keys[i], hashCodes[i]);
      assertTrue(Arrays.equals(value, values[i]));
    }

    store.rehash(accessor, 40);
    assertEquals(1, accessor.getHashtable().getVersionId());

    for (int i=0; i< 256; i++) {
      byte[] value = store.get(accessor, keys[i], hashCodes[i]);
      assertTrue(Arrays.equals(value, values[i]));
    }

    store.rehash(accessor, 12345);
    assertEquals(2, accessor.getHashtable().getVersionId());

    for (int i=0; i< 256; i++) {
      byte[] value = store.get(accessor, keys[i], hashCodes[i]);
      assertTrue(Arrays.equals(value, values[i]));
    }

    store.rehash(accessor, 4);
    assertEquals(3, accessor.getHashtable().getVersionId());

    for (int i=0; i< 256; i++) {
      byte[] value = store.get(accessor, keys[i], hashCodes[i]);
      assertTrue(Arrays.equals(value, values[i]));
    }

    store.returnAccessor(accessor);

    store.close();

  }
}
