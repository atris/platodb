package org.atri.platodb.store;

import org.junit.Test;
import org.atri.platodb.store.data.platotrie.KeysPartition;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * @author atri
 * @since 2017-jun-29 01:04:22
 */
public class TestCursor extends StoreTest {

  @Test
  public void testCursor() throws IOException {

    Charset utf8 = Charset.forName("UTF8");

    Configuration configuration = new Configuration(getDirectory("testCursor"));
    configuration.setInitialCapacity(100);
    configuration.setHashCodesPartitionByteSize(10000);
    configuration.setKeysPartitionByteSize(10000);
    configuration.setValuesPartitionByteSize(10000);
    Store store = new Store(configuration);
    store.open();

    long[] keyHashCodes = new long[]{
        1, 2, 3, 4
    };

    byte[][] keys = new byte[][]{
        new byte[]{0x01},
        new byte[]{0x02},
        new byte[]{0x03},
        new byte[]{0x04},
    };

    byte[][] values = new byte[][]{
        "one".getBytes(utf8),
        "two".getBytes(utf8),
        "three".getBytes(utf8),
        "four".getBytes(utf8),
    };

    Accessor accessor = store.borrowAccessor();
    for (int i = 0; i < 4; i++) {
      store.put(accessor, keys[i], keyHashCodes[i], values[i], 0);
    }

    Cursor<KeysPartition.Posting> cursor = store.keys();

    KeysPartition.Posting posting = new KeysPartition.Posting();
    for (int i = 0; i < 4; i++) {
      posting = cursor.next(accessor, posting, 0);
      assertNotNull(posting);
      assertTrue("At position " + i, Arrays.equals(keys[i], posting.getBytes()));
    }
    assertNull(cursor.next(accessor, posting, 0));

    store.returnAccessor(accessor);

    store.close();
  }

}
