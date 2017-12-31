package org.atri.platodb.store.data.platotrie;

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
import org.atri.platodb.store.data.FileHandler;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.File;

/**
 * Hashtable file. There is never more than one of these that are valid at any given time.
 * <p/>
 * The position in the hashtable for a given hash code is calculated as (hash & (capacity - 1)).
 * At this position there is a posting that points at the first known hash code posting.
 * <p/>
 * This file is affected by rehashing.
 *
 * @author atri
 * @since 2017-mar-16 14:00:56
 */
public class Hashtable extends FileHandler<Hashtable.Header, Hashtable.Posting> {


  private int versionId;

  /**
   * header as when file was openend. used only to read capacity. but that is also all the header contains..
   */
  private Header header;

  public Hashtable(File directory, int versionId, String access, LockFactory lockFactory) throws IOException {
    super(directory, versionId, "ht", access, lockFactory);
    this.versionId = versionId;
  }

  @Override
  public void open() throws IOException {
    super.open();
    readHeader(header = new Header());
  }

  public int getVersionId() {
    return versionId;
  }

  public static final int HEADER_BYTE_SIZE = 1024;

  public int getHeaderByteSize() {
    return HEADER_BYTE_SIZE;
  }

  public static class Header extends FileHandler.Header {
    /**
     * This hashtable postings file capacity.
     */
    private int postingsCapacity;

    public int getPostingsCapacity() {
      return postingsCapacity;
    }

    public void setPostingsCapacity(int postingsCapacity) {
      this.postingsCapacity = postingsCapacity;
    }
  }


  public static class Posting extends FileHandler.Posting {

    public static final int POSTING_BYTE_SIZE = 1 + 8 + 4 + 4 + 8;

    public int getPostingByteSize() {
      return POSTING_BYTE_SIZE;
    }

    /**
     * 0 = never used
     * 1 = in use
     * 2 = deleted
     */
    private byte flag;

    private long createdRevision;

    /**
     * Partition id of first hash code posting with this hashtable position. -1 == null
     */
    private int hashCodePostingPartition;

    /**
     * Offset in above hash code postings partition.
     */
    private int hashCodePostingPartitionOffset;

    private long deletedRevision = -1;

    public byte getFlag() {
      return flag;
    }

    public void setFlag(byte flag) {
      this.flag = flag;
    }

    public int getHashCodePostingPartition() {
      return hashCodePostingPartition;
    }

    public void setHashCodePostingPartition(int hashCodePostingPartition) {
      this.hashCodePostingPartition = hashCodePostingPartition;
    }

    public int getHashCodePostingPartitionOffset() {
      return hashCodePostingPartitionOffset;
    }

    public void setHashCodePostingPartitionOffset(int hashCodePostingPartitionOffset) {
      this.hashCodePostingPartitionOffset = hashCodePostingPartitionOffset;
    }

    public long getCreatedRevision() {
      return createdRevision;
    }

    public void setCreatedRevision(long createdRevision) {
      this.createdRevision = createdRevision;
    }

    public long getDeletedRevision() {
      return deletedRevision;
    }

    public void setDeletedRevision(long deletedRevision) {
      this.deletedRevision = deletedRevision;
    }
  }

  public void readHeader(Header header, RandomAccessFile RAF) throws IOException {
    header.postingsCapacity = RAF.readInt();
  }

  public void writeHeader(Header header, RandomAccessFile RAF) throws IOException {
    RAF.writeInt(header.postingsCapacity);
    this.header = new Header();
    this.header.postingsCapacity = header.postingsCapacity;
  }

  public int calculateHashCodePostingOffset(long hashCode) {
    return (int) (HEADER_BYTE_SIZE + (Posting.POSTING_BYTE_SIZE * (hashCode & (header.postingsCapacity - 1))));
  }

  public void readPosting(Posting posting, RandomAccessFile RAF) throws IOException {
    posting.flag = RAF.readByte();
    posting.createdRevision = RAF.readLong();
    posting.hashCodePostingPartition = RAF.readInt();
    posting.hashCodePostingPartitionOffset = RAF.readInt();
    posting.deletedRevision = RAF.readLong();
  }

  public void writePosting(Posting posting, RandomAccessFile RAF) throws IOException {
    RAF.writeByte(posting.flag);
    RAF.writeLong(posting.createdRevision);
    RAF.writeInt(posting.hashCodePostingPartition);
    RAF.writeInt(posting.hashCodePostingPartitionOffset);
    RAF.writeLong(posting.deletedRevision);
  }

  public void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException {
    RAF.seek(startOffset);
    RAF.writeByte((byte) 2);
    RAF.skipBytes(8 + 4 + 4);
    RAF.writeLong(revision);
  }

}
