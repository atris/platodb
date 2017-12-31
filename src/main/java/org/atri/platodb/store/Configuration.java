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


import org.atri.platodb.store.lock.LockFactory;
import org.atri.platodb.store.lock.NativeFSLockFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author atri
 * @since 2017-mar-16 15:53:03
 */
public class Configuration {

  /**
   * Location of directory containing the data files.
   * Only one hashtable can exist in a path.
   */
  private File dataPath;

  public Configuration(File dataPath) throws IOException {
    this.dataPath = dataPath;
    lockFactory = new NativeFSLockFactory(dataPath);
  }

  /**
   * Durability as in D of ACID.
   *
   * In order to retain durability
   * postings in the files of a store are never deleted,
   * they are flagged as in use, deleted or never used.
   *
   * Durable posting links are pointers to data in chained posting files
   * (keys with same hash code and hash codes with same hashtable position)
   * that points at data deleted or replaced in earlier revisions.
   * They make it possible to instantly access the store as if it was any given revision.
   *
   *
   * This will have very little effect on the speed for stores
   * that contains few deletions and changes
   * and quite a lot if it contains many changes,
   * as it means more I/O seek.
   *
   * Go SSD!
   */
  private boolean usingDurablePostingLinks = false;

  /**
   * Number of items you want to fit in the hashtable from the start.
   */
  private int initialCapacity = 8000000;

  /**
   * The factor of which to grow the capacity on rehash.
   */
  private double automaticRehashCapacityGrowFactor = 1.7d;

  /**
   * Ratio of available key posting left required to trigger a rehash at put()-time.
   * <p/>
   * The default identified sweetspot
   * is to have at least 8x greater capacity than items in the hashtable.
   * <p/>
   * In order to automatically rehash so you never fill the hashtable to more than 1/8
   * you have set this value to 1/8 (0.125).
   *
   * A number greater than 0 and less than 1.
   */
  private double automaticRehashThreadshold = 0.125d;

  private LockFactory lockFactory;

  private long lockWaitTimeoutMilliseconds = 60000;

  public static final int megaByte = 1024 * 1024;

  private int valuesPartitionByteSize = 100 * megaByte;
  private int keysPartitionByteSize = 50 * megaByte;
  private int hashCodesPartitionByteSize = 25 * megaByte;


  /**
   * Durability as in D of ACID.
   *
   * In order to retain durability
   * postings in the files of a store are never deleted,
   * they are flagged as in use, deleted or never used.
   *
   * Durable posting links are pointers to data in chained posting files
   * (keys with same hash code and hash codes with same hashtable position)
   * that points at data deleted or replaced in earlier revisions.
   * They make it possible to instantly access the store as if it was any given revision.
   *
   *
   * This will have very little effect on the speed for stores
   * that contains few deletions and changes
   * and quite a lot if it contains many changes,
   * as it means more I/O seek.
   *
   * Go SSD!
   */
  public boolean isUsingDurablePostingLinks() {
    return usingDurablePostingLinks;
  }

  /**
   * Durability as in D of ACID.
   *
   * In order to retain durability
   * postings in the files of a store are never deleted,
   * they are flagged as in use, deleted or never used.
   *
   * Durable posting links are pointers to data in chained posting files
   * (keys with same hash code and hash codes with same hashtable position)
   * that points at data deleted or replaced in earlier revisions.
   * They make it possible to instantly access the store as if it was any given revision.
   *
   *
   * This will have very little effect on the speed for stores
   * that contains few deletions and changes
   * and quite a lot if it contains many changes,
   * as it means more I/O seek.
   *
   * Go SSD!
   */
  public void setUsingDurablePostingLinks(boolean usingDurablePostingLinks) {
    this.usingDurablePostingLinks = usingDurablePostingLinks;
  }

  public int getInitialCapacity() {
    return initialCapacity;
  }

  public void setInitialCapacity(int initialCapacity) {
    this.initialCapacity = initialCapacity;
  }

  public double getAutomaticRehashCapacityGrowFactor() {
    return automaticRehashCapacityGrowFactor;
  }

  public void setAutomaticRehashCapacityGrowFactor(double automaticRehashCapacityGrowFactor) {
    this.automaticRehashCapacityGrowFactor = automaticRehashCapacityGrowFactor;
  }

  public double getAutomaticRehashThreadshold() {
    return automaticRehashThreadshold;
  }

  public void setAutomaticRehashThreadshold(double automaticRehashThreadshold) {
    this.automaticRehashThreadshold = automaticRehashThreadshold;
  }

  public LockFactory getLockFactory() {
    return lockFactory;
  }

  public void setLockFactory(LockFactory lockFactory) {
    this.lockFactory = lockFactory;
  }

  public long getLockWaitTimeoutMilliseconds() {
    return lockWaitTimeoutMilliseconds;
  }

  public void setLockWaitTimeoutMilliseconds(long lockWaitTimeoutMilliseconds) {
    this.lockWaitTimeoutMilliseconds = lockWaitTimeoutMilliseconds;
  }

  public int getValuesPartitionByteSize() {
    return valuesPartitionByteSize;
  }

  public void setValuesPartitionByteSize(int valuesPartitionByteSize) {
    this.valuesPartitionByteSize = valuesPartitionByteSize;
  }

  public int getKeysPartitionByteSize() {
    return keysPartitionByteSize;
  }

  public void setKeysPartitionByteSize(int keysPartitionByteSize) {
    this.keysPartitionByteSize = keysPartitionByteSize;
  }

  public int getHashCodesPartitionByteSize() {
    return hashCodesPartitionByteSize;
  }

  public void setHashCodesPartitionByteSize(int hashCodesPartitionByteSize) {
    this.hashCodesPartitionByteSize = hashCodesPartitionByteSize;
  }

  public File getDataPath() {
    return dataPath;
  }

  public void setDataPath(File dataPath) {
    this.dataPath = dataPath;
  }
}
