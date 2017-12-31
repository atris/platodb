package org.atri.platodb.store.data;

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

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.File;

/**
 * @author atri
 * @since 2017-mar-16 14:16:39
 */
public class Metadata extends FileHandler<Metadata.Header, FileHandler.Posting> {

  public Metadata(File directory, String access, LockFactory lockFactory) throws IOException {
    super(directory, 0, "md", access, lockFactory);
  }

  public static final int HEADER_BYTE_SIZE = 1024;
  public int getHeaderByteSize() {
    return HEADER_BYTE_SIZE;
  }

  public static class Header extends FileHandler.Header {
    /**
     * File format version.
     */
    private int fileFormatVersion;

    /**
     * Current hashtable file id. -- will change after rehash.
     */
    private int currentHashtableId;

    /** Current hash codes partition used for appending new postings */
    private int currentHashCodesPartition;

    /** Current keys partition used for appending new postings */
    private int currentKeysPartition;

    /** Current values partition used for appending new postings */
    private int currentValuesPartition;

    /**
     * Total number of value postings.
     */
    private long valuePostingsCount;

    /**
     * Commit version, will increase by one after each modification to the database.
     */
    private long storeRevision;

    public int getCurrentHashCodesPartition() {
      return currentHashCodesPartition;
    }

    public void setCurrentHashCodesPartition(int currentHashCodesPartition) {
      this.currentHashCodesPartition = currentHashCodesPartition;
    }

    public int getCurrentKeysPartition() {
      return currentKeysPartition;
    }

    public void setCurrentKeysPartition(int currentKeysPartition) {
      this.currentKeysPartition = currentKeysPartition;
    }

    public int getCurrentValuesPartition() {
      return currentValuesPartition;
    }

    public void setCurrentValuesPartition(int currentValuesPartition) {
      this.currentValuesPartition = currentValuesPartition;
    }

    public int getFileFormatVersion() {
      return fileFormatVersion;
    }

    public void setFileFormatVersion(int fileFormatVersion) {
      this.fileFormatVersion = fileFormatVersion;
    }

    public int getCurrentHashtableId() {
      return currentHashtableId;
    }

    public void setCurrentHashtableId(int currentHashtableId) {
      this.currentHashtableId = currentHashtableId;
    }

    public long getValuePostingsCount() {
      return valuePostingsCount;
    }

    public void setValuePostingsCount(long valuePostingsCount) {
      this.valuePostingsCount = valuePostingsCount;
    }

    public long getStoreRevision() {
      return storeRevision;
    }

    public void setStoreRevision(long storeRevision) {
      this.storeRevision = storeRevision;
    }

    public long increaseRevision(long value) {
      return storeRevision += value;
    }

    public long increaseValuePostingsCount(long value) {
      return valuePostingsCount += value;
    }

    public long decreaseValuePostingsCount(long value) {
      return valuePostingsCount -= value;
    }

  }

  public void readHeader(Header header, RandomAccessFile RAF) throws IOException {
    header.fileFormatVersion = RAF.readInt();
    header.storeRevision = RAF.readLong();
    header.currentHashtableId = RAF.readInt();
    header.currentHashCodesPartition = RAF.readInt();
    header.currentKeysPartition = RAF.readInt();
    header.currentValuesPartition = RAF.readInt();
    header.valuePostingsCount = RAF.readLong();
    System.currentTimeMillis();
  }

  public void writeHeader(Header header, RandomAccessFile RAF) throws IOException {
    RAF.writeInt(header.fileFormatVersion);
    RAF.writeLong(header.storeRevision);
    RAF.writeInt(header.currentHashtableId);
    RAF.writeInt(header.currentHashCodesPartition);
    RAF.writeInt(header.currentKeysPartition);
    RAF.writeInt(header.currentValuesPartition);
    RAF.writeLong(header.valuePostingsCount);
  }

  public void writePosting(Posting posting, RandomAccessFile RAF) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readPosting(Posting posting, RandomAccessFile RAF) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException {
    throw new UnsupportedOperationException();
  }
}
