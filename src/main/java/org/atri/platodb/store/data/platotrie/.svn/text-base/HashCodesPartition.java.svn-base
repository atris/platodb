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
 * Hash code postings partition file.
 * <p/>
 * Chained postings. Each postings has a unique hash code value and points at how to find
 * the key posting for this hash code.
 * <p/>
 * This file is affected by rehashing.
 *
 * @author atri
 * @since 2017-mar-16 14:00:37
 */
public class HashCodesPartition extends FileHandler<HashCodesPartition.Header, HashCodesPartition.Posting> {

  private int partitionId;

  public HashCodesPartition(File directory, int partitionId, String access, LockFactory lockFactory) throws IOException {
    super(directory, partitionId, "hc", access, lockFactory);
    this.partitionId = partitionId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public static final int HEADER_BYTE_SIZE = 1024;

  public int getHeaderByteSize() {
    return HEADER_BYTE_SIZE;
  }

  public static class Header extends FileHandler.Header {

    /**
     * Offset in this partition for next new posting.
     */
    private int nextPostingOffset;
    /**
     * Bytes left for use in this partition.
     */
    private int bytesLeft;

    public int getNextPostingOffset() {
      return nextPostingOffset;
    }

    public void setNextPostingOffset(int nextPostingOffset) {
      this.nextPostingOffset = nextPostingOffset;
    }

    public int getBytesLeft() {
      return bytesLeft;
    }

    public void setBytesLeft(int bytesLeft) {
      this.bytesLeft = bytesLeft;
    }


  }

  public static final int POSTING_BYTE_SIZE = 1 + 8 + 8 + 4 + 4 + 4 + 4 + 8;

  public static class Posting extends FileHandler.Posting {

    public int getPostingByteSize() {
      return POSTING_BYTE_SIZE;
    }

    /**
     * 0 = never used
     * 1 = in use
     * 2 = deleted
     */
    private byte flag;

    /**
     * Key hash code.
     */
    private long keyHashCode;

    private long createdRevision;


    /**
     * Partition id of next hash code posting with the same hashtable posting position.
     * -1 == no more hash code postings in chain
     */
    private int nextPostingPartition;

    /**
     * Offset in above hash code postings partition.
     */
    private int nextPostingPartitionOffset;


    /**
     * Partition id of first key posting with this hash code.
     */
    private int firstKeyPostingPartition;

    /**
     * Offset in above key postings partition.
     */
    private int firstKeyPostingPartitionOffset;

    private long deletedRevision = -1;


    public byte getFlag() {
      return flag;
    }

    public void setFlag(byte flag) {
      this.flag = flag;
    }

    public long getKeyHashCode() {
      return keyHashCode;
    }

    public void setKeyHashCode(long keyHashCode) {
      this.keyHashCode = keyHashCode;
    }

    public int getNextPostingPartition() {
      return nextPostingPartition;
    }

    public void setNextPostingPartition(int nextPostingPartition) {
      this.nextPostingPartition = nextPostingPartition;
    }

    public int getNextPostingPartitionOffset() {
      return nextPostingPartitionOffset;
    }

    public void setNextPostingPartitionOffset(int nextPostingPartitionOffset) {
      this.nextPostingPartitionOffset = nextPostingPartitionOffset;
    }

    public int getFirstKeyPostingPartition() {
      return firstKeyPostingPartition;
    }

    public void setFirstKeyPostingPartition(int firstKeyPostingPartition) {
      this.firstKeyPostingPartition = firstKeyPostingPartition;
    }

    public int getFirstKeyPostingPartitionOffset() {
      return firstKeyPostingPartitionOffset;
    }

    public void setFirstKeyPostingPartitionOffset(int firstKeyPostingPartitionOffset) {
      this.firstKeyPostingPartitionOffset = firstKeyPostingPartitionOffset;
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
    header.nextPostingOffset = RAF.readInt();
    header.bytesLeft = RAF.readInt();
  }

  public void writeHeader(Header header, RandomAccessFile RAF) throws IOException {
    RAF.writeInt(header.nextPostingOffset);
    RAF.writeInt(header.bytesLeft);
  }


  public void readPosting(Posting posting, RandomAccessFile RAF) throws IOException {
    posting.flag = RAF.readByte();
    posting.keyHashCode = RAF.readLong();
    posting.createdRevision = RAF.readLong();
    posting.nextPostingPartition = RAF.readInt();
    posting.nextPostingPartitionOffset = RAF.readInt();
    posting.firstKeyPostingPartition = RAF.readInt();
    posting.firstKeyPostingPartitionOffset = RAF.readInt();
    posting.deletedRevision = RAF.readLong();
  }

  public void writePosting(Posting posting, RandomAccessFile RAF) throws IOException {
    if (posting.flag == Posting.FLAG_DELETED
        && posting.deletedRevision == -1) {
      System.currentTimeMillis();
    }
    RAF.writeByte(posting.flag);
    RAF.writeLong(posting.keyHashCode);
    RAF.writeLong(posting.createdRevision);
    RAF.writeInt(posting.nextPostingPartition);
    RAF.writeInt(posting.nextPostingPartitionOffset);
    RAF.writeInt(posting.firstKeyPostingPartition);
    RAF.writeInt(posting.firstKeyPostingPartitionOffset);
    RAF.writeLong(posting.deletedRevision);
  }

  public void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException {
    if (revision == -1) {
      System.currentTimeMillis();
    }
    RAF.seek(startOffset);
    RAF.writeByte(Posting.FLAG_DELETED);
    RAF.skipBytes(8 + 8 + 4 + 4 + 4 + 4);
    RAF.writeLong(revision);
  }
}
