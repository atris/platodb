package org.atri.platodb.store;

/*
 *@author atri
 * Licensed to PlatoDB
 * 
 *
 * 
 *
 *
 *    
 *
 *
 *
 *
 *
 *.
 */


import org.junit.Test;
import org.atri.platodb.store.data.platotrie.Hashtable;
import org.atri.platodb.store.data.platotrie.HashCodesPartition;
import org.atri.platodb.store.data.platotrie.KeysPartition;
import org.atri.platodb.store.data.platotrie.ValuesPartition;
import org.atri.platodb.store.data.FileHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author atri
 * @since 2017-mar-17 00:48:33
 */
public class TestStoreAccessors extends StoreTest {



  @Test
  public void testSimple() throws IOException {

    Configuration configuration = new Configuration(getDirectory("testStoreAccessors"));
    Store store = new Store(configuration);
    store.open();

    Accessor accessor;

//    // set read only
//    try {
//      store.createAccessor(true);
//      fail("Accessor is set read only! Store should be be created");
//    } catch (IOException ioe) {
//      // all good
//    }

    // create the store
    store.returnAccessor(store.borrowAccessor());

    byte[] key;
    long hash;
    byte[] value;


    key = new byte[10];
    Arrays.fill(key, (byte) 1);
    hash = 10l;
    value = new byte[100];
    Arrays.fill(value, (byte) 2);


//    // open read only again
//    accessor = store.borrowAccessor();
//
//    assertFalse(store.containsKey(accessor, key, hash));
//    try {
//      assertNull(store.put(accessor, key, hash, value, 1l));
//      fail("Accessor is set read only and should not be able to add entiteis!");
//    } catch (IOException ioe) {
//      // all good
//      accessor.close();
//    }

    // open read/write
    accessor = store.borrowAccessor();

    // simple general put/get/containsKey/remove tests

    assertNull(store.put(accessor, key, hash, value, 1l));
    assertTrue(store.containsKey(accessor, key, hash));
    assertTrue(Arrays.equals(value, store.get(accessor, key, hash)));
    // add again with alternative values
    byte[] value2 = new byte[100];
    Arrays.fill(value2, (byte) 3);
    assertTrue(Arrays.equals(value, store.put(accessor, key, hash, value2, 1l)));
    assertTrue("It is probable that the byte array in ValuePartition.Posting (value2) was attempted to be cached rather than cloned and thus overwritten",
    Arrays.equals(value2, store.remove(accessor, key, hash, 1l)));
    assertFalse(store.containsKey(accessor, key, hash));

    assertNull(store.put(accessor, key, hash, value2, 1l));
    assertTrue(store.containsKey(accessor, key, hash));

    // alternative values, new hash

    key = new byte[10];
    Arrays.fill(key, (byte) 4);
    hash = 11l;
    value = new byte[100];
    Arrays.fill(value, (byte) 5);

    assertFalse(store.containsKey(accessor, key, hash));
    assertNull(store.put(accessor, key, hash, value, 1l));
    assertTrue(store.containsKey(accessor, key, hash));
    assertTrue(Arrays.equals(value, store.get(accessor, key, hash)));


    key = new byte[10];
    Arrays.fill(key, (byte) 5);
    hash = 12l;
    value = new byte[100];
    Arrays.fill(value, (byte) 6);

    assertFalse(store.containsKey(accessor, key, hash));
    assertNull(store.put(accessor, key, hash, value, 1l));
    assertTrue(store.containsKey(accessor, key, hash));
    assertTrue(Arrays.equals(value, store.get(accessor, key, hash)));
    assertTrue(Arrays.equals(value, store.remove(accessor, key, hash, 1l)));
    assertFalse(store.containsKey(accessor, key, hash));

    store.returnAccessor(accessor);

    store.close();

  }
}
