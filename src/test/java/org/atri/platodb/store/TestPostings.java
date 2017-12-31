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

/**
 * @author atri
 * @since 2017-mar-17 00:48:33
 */
public class TestPostings extends StoreTest {


  public void testDurablePostings() throws IOException {

    randomizeKeyValues(1000);

    Configuration configuration = new Configuration(getDirectory("testDurablePostings"));
    configuration.setUsingDurablePostingLinks(true);
    Store store = new Store(configuration);
    store.open();

    Accessor accessor;


    accessor = store.borrowAccessor();
    long revision = 1;

    store.put(accessor, keys[0], hashes[0], values[0], revision);

    Hashtable.Posting htp = new Hashtable.Posting();
    accessor.getHashtable().readPosting(htp, accessor.getHashtable().calculateHashCodePostingOffset(hashes[0]));

    assertEquals(FileHandler.Posting.FLAG_IN_USE, htp.getFlag());
    assertEquals(revision, htp.getCreatedRevision());
    assertEquals(-1, htp.getDeletedRevision());

    HashCodesPartition.Posting hcp = new HashCodesPartition.Posting();
    accessor.getHashCodesPartition(htp.getHashCodePostingPartition()).readPosting(hcp, htp.getHashCodePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, hcp.getFlag());
    assertEquals(revision, hcp.getCreatedRevision());
    assertEquals(-1, hcp.getDeletedRevision());
    assertEquals(-1, hcp.getNextPostingPartition());
    assertEquals(-1, hcp.getNextPostingPartitionOffset());
    assertEquals(hashes[0], hcp.getKeyHashCode());

    KeysPartition.Posting kp = new KeysPartition.Posting();
    accessor.getKeysPartition(hcp.getFirstKeyPostingPartition(), true).readPosting(kp, hcp.getFirstKeyPostingPartitionOffset());


    assertEquals(FileHandler.Posting.FLAG_IN_USE, kp.getFlag());
    assertEquals(revision, kp.getCreatedRevision());
    assertEquals(-1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());

    ValuesPartition.Posting vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, vp.getFlag());
    assertEquals(revision, vp.getCreatedRevision());
    assertEquals(-1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[0], vp.getBytes()));

    // add second posting with same key, check updates
    revision++;

    store.put(accessor, keys[0], hashes[0], values[1], revision);

    htp = new Hashtable.Posting();
    accessor.getHashtable().readPosting(htp, accessor.getHashtable().calculateHashCodePostingOffset(hashes[0]));

    assertEquals(FileHandler.Posting.FLAG_IN_USE, htp.getFlag());
    assertEquals(revision - 1, htp.getCreatedRevision());
    assertEquals(-1, htp.getDeletedRevision());

    hcp = new HashCodesPartition.Posting();
    accessor.getHashCodesPartition(htp.getHashCodePostingPartition()).readPosting(hcp, htp.getHashCodePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, hcp.getFlag());
    assertEquals(revision - 1, hcp.getCreatedRevision());
    assertEquals(-1, hcp.getDeletedRevision());
    assertEquals(-1, hcp.getNextPostingPartition());
    assertEquals(-1, hcp.getNextPostingPartitionOffset());
    assertEquals(hashes[0], hcp.getKeyHashCode());

    kp = new KeysPartition.Posting();
    accessor.getKeysPartition(hcp.getFirstKeyPostingPartition(), true).readPosting(kp, hcp.getFirstKeyPostingPartitionOffset());

    // the new posting first

    assertEquals(FileHandler.Posting.FLAG_IN_USE, kp.getFlag());
    assertEquals(revision, kp.getCreatedRevision());
    assertEquals(-1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertTrue(-1 < kp.getNextKeyPostingPartition());
    assertTrue(-1 < kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, vp.getFlag());
    assertEquals(revision, vp.getCreatedRevision());
    assertEquals(-1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[1], vp.getBytes()));


    // then the old posting
    accessor.getKeysPartition(kp.getNextKeyPostingPartition(), true).readPosting(kp, kp.getNextKeyPostingPartitionOffset());
    assertEquals(FileHandler.Posting.FLAG_DELETED, kp.getFlag());
    assertEquals(revision - 1, kp.getCreatedRevision());
    assertEquals(revision, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertEquals(-1, kp.getNextKeyPostingPartition());
    assertEquals(-1, kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_DELETED, vp.getFlag());
    assertEquals(revision - 1, vp.getCreatedRevision());
    assertEquals(revision, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[0], vp.getBytes()));


    // add a third

    revision++;
    store.put(accessor, keys[0], hashes[0], values[2], revision);


    htp = new Hashtable.Posting();
    accessor.getHashtable().readPosting(htp, accessor.getHashtable().calculateHashCodePostingOffset(hashes[0]));

    assertEquals(FileHandler.Posting.FLAG_IN_USE, htp.getFlag());
    assertEquals(revision - 2, htp.getCreatedRevision());
    assertEquals(-1, htp.getDeletedRevision());

    hcp = new HashCodesPartition.Posting();
    accessor.getHashCodesPartition(htp.getHashCodePostingPartition()).readPosting(hcp, htp.getHashCodePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, hcp.getFlag());
    assertEquals(revision - 2, hcp.getCreatedRevision());
    assertEquals(-1, hcp.getDeletedRevision());
    assertEquals(-1, hcp.getNextPostingPartition());
    assertEquals(-1, hcp.getNextPostingPartitionOffset());
    assertEquals(hashes[0], hcp.getKeyHashCode());

    kp = new KeysPartition.Posting();
    accessor.getKeysPartition(hcp.getFirstKeyPostingPartition(), true).readPosting(kp, hcp.getFirstKeyPostingPartitionOffset());

    // the new posting first

    assertEquals(FileHandler.Posting.FLAG_IN_USE, kp.getFlag());
    assertEquals(revision, kp.getCreatedRevision());
    assertEquals(-1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertTrue(-1 < kp.getNextKeyPostingPartition());
    assertTrue(-1 < kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, vp.getFlag());
    assertEquals(revision, vp.getCreatedRevision());
    assertEquals(-1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[2], vp.getBytes()));


    // then the older posting
    accessor.getKeysPartition(kp.getNextKeyPostingPartition(), true).readPosting(kp, kp.getNextKeyPostingPartitionOffset());
    assertEquals(FileHandler.Posting.FLAG_DELETED, kp.getFlag());
    assertEquals(revision - 1, kp.getCreatedRevision());
    assertEquals(revision, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertTrue(-1 < kp.getNextKeyPostingPartition());
    assertTrue(-1 < kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_DELETED, vp.getFlag());
    assertEquals(revision - 1, vp.getCreatedRevision());
    assertEquals(revision, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[1], vp.getBytes()));

    // then the oldest posting
    accessor.getKeysPartition(kp.getNextKeyPostingPartition(), true).readPosting(kp, kp.getNextKeyPostingPartitionOffset());
    assertEquals(FileHandler.Posting.FLAG_DELETED, kp.getFlag());
    assertEquals(revision - 2, kp.getCreatedRevision());
    assertEquals(revision - 1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertEquals(-1, kp.getNextKeyPostingPartition());
    assertEquals(-1, kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_DELETED, vp.getFlag());
    assertEquals(revision - 2, vp.getCreatedRevision());
    assertEquals(revision - 1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[0], vp.getBytes()));


    store.returnAccessor(accessor);

    store.close();
  }


  @Test
  public void testPostings() throws IOException {

    randomizeKeyValues(1000);

    Configuration configuration = new Configuration(getDirectory("testPostings"));
    configuration.setUsingDurablePostingLinks(false);
    Store store = new Store(configuration);
    store.open();

    Accessor accessor;


    accessor = store.borrowAccessor();
    long revision = 1;

    store.put(accessor, keys[0], hashes[0], values[0], revision);

    Hashtable.Posting htp = new Hashtable.Posting();
    accessor.getHashtable().readPosting(htp, accessor.getHashtable().calculateHashCodePostingOffset(hashes[0]));

    assertEquals(FileHandler.Posting.FLAG_IN_USE, htp.getFlag());
    assertEquals(revision, htp.getCreatedRevision());
    assertEquals(-1, htp.getDeletedRevision());

    HashCodesPartition.Posting hcp = new HashCodesPartition.Posting();
    accessor.getHashCodesPartition(htp.getHashCodePostingPartition()).readPosting(hcp, htp.getHashCodePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, hcp.getFlag());
    assertEquals(revision, hcp.getCreatedRevision());
    assertEquals(-1, hcp.getDeletedRevision());
    assertEquals(-1, hcp.getNextPostingPartition());
    assertEquals(-1, hcp.getNextPostingPartitionOffset());
    assertEquals(hashes[0], hcp.getKeyHashCode());

    KeysPartition.Posting kp = new KeysPartition.Posting();
    accessor.getKeysPartition(hcp.getFirstKeyPostingPartition(), true).readPosting(kp, hcp.getFirstKeyPostingPartitionOffset());


    assertEquals(FileHandler.Posting.FLAG_IN_USE, kp.getFlag());
    assertEquals(revision, kp.getCreatedRevision());
    assertEquals(-1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());

    ValuesPartition.Posting vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, vp.getFlag());
    assertEquals(revision, vp.getCreatedRevision());
    assertEquals(-1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[0], vp.getBytes()));

    // add second posting with same key, check updates
    revision++;

    store.put(accessor, keys[0], hashes[0], values[1], revision);

    htp = new Hashtable.Posting();
    accessor.getHashtable().readPosting(htp, accessor.getHashtable().calculateHashCodePostingOffset(hashes[0]));

    assertEquals(FileHandler.Posting.FLAG_IN_USE, htp.getFlag());
    assertEquals(revision - 1, htp.getCreatedRevision());
    assertEquals(-1, htp.getDeletedRevision());

    hcp = new HashCodesPartition.Posting();
    accessor.getHashCodesPartition(htp.getHashCodePostingPartition()).readPosting(hcp, htp.getHashCodePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, hcp.getFlag());
    assertEquals(revision - 1, hcp.getCreatedRevision());
    assertEquals(-1, hcp.getDeletedRevision());
    assertEquals(-1, hcp.getNextPostingPartition());
    assertEquals(-1, hcp.getNextPostingPartitionOffset());
    assertEquals(hashes[0], hcp.getKeyHashCode());

    kp = new KeysPartition.Posting();
    accessor.getKeysPartition(hcp.getFirstKeyPostingPartition(), true).readPosting(kp, hcp.getFirstKeyPostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, kp.getFlag());
    assertEquals(revision, kp.getCreatedRevision());
    assertEquals(-1, kp.getDeletedRevision());
    assertTrue(Arrays.equals(keys[0], kp.getBytes()));
    assertEquals(hashes[0], kp.getKeyHashCode());
    assertEquals(-1, kp.getNextKeyPostingPartition());
    assertEquals(-1, kp.getNextKeyPostingPartitionOffset());

    vp = new ValuesPartition.Posting();
    accessor.getValuesPartition(kp.getValuePostingPartition()).readPosting(vp, kp.getValuePostingPartitionOffset());

    assertEquals(FileHandler.Posting.FLAG_IN_USE, vp.getFlag());
    assertEquals(revision, vp.getCreatedRevision());
    assertEquals(-1, vp.getDeletedRevision());
    assertTrue(Arrays.equals(values[1], vp.getBytes()));


    store.returnAccessor(accessor);
    // todo test delete postings

    store.close();
  }


}