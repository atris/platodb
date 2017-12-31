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


import org.atri.platodb.store.data.FileHandler;
import org.atri.platodb.store.lock.LockFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Key postings partition file.
 * <p/>
 * Chained postings. Each posting contains a unique key value and points at how to
 * find the value associated with this key.
 * <p/>
 * This file is NOT affected by rehashing.
 *
 * @author atri
 * @since 2017-mar-16 14:00:22
 */
public class KeysPartition extends FileHandler<KeysPartition.Header, KeysPartition.Posting> {

  private int partitionId;

  public KeysPartition(File directory, int partitionId, String access, LockFactory lockFactory) throws IOException {
    super(directory, partitionId, "k", access, lockFactory);
    this.partitionId = partitionId;
  }

  public static final int HEADER_BYTE_SIZE = 1024;

  public int getHeaderByteSize() {
    return HEADER_BYTE_SIZE;
  }

  public int getPartitionId() {
    return partitionId;
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

  public static class Posting extends FileHandler.Posting {


    /**
     * 0 = never used
     * 1 = in use
     * 2 = deleted
     */
    private byte flag;

    private long createdRevision;

    /**
     * Partition id of next key posting with the same hash code.
     * -1 == end of keys chain
     * -2 == deleted key
     */
    private int nextKeyPostingPartition;
    /**
     * Offset in above key postings partition.
     */
    private int nextKeyPostingPartitionOffset;

    /**
     * Key hash code
     */
    private long keyHashCode;

    /**
     * Paritition id of value posting. -1 == null
     */
    private int valuePostingPartition;
    /**
     * Offset in above value postings partition.
     */
    private int valuePostingPartitionOffset;

    /**
     * Length in bytes of serialized key.
     */
    private int bytesLength;
    /**
     * Serialized key
     */
    private byte[] bytes;

    private long deletedRevision = -1;

    public int getPostingByteSize() {
      return 1 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + bytesLength + 8;
    }

    /**
     * 0 = never used
     * 1 = in use
     * 2 = deleted
     */
    public byte getFlag() {
      return flag;
    }

    public void setFlag(byte flag) {
      this.flag = flag;
    }

    public int getNextKeyPostingPartition() {
      return nextKeyPostingPartition;
    }

    public void setNextKeyPostingPartition(int nextKeyPostingPartition) {
      this.nextKeyPostingPartition = nextKeyPostingPartition;
    }

    public int getNextKeyPostingPartitionOffset() {
      return nextKeyPostingPartitionOffset;
    }

    public void setNextKeyPostingPartitionOffset(int nextKeyPostingPartitionOffset) {
      this.nextKeyPostingPartitionOffset = nextKeyPostingPartitionOffset;
    }

    public long getKeyHashCode() {
      return keyHashCode;
    }

    public void setKeyHashCode(long keyHashCode) {
      this.keyHashCode = keyHashCode;
    }

    public int getValuePostingPartition() {
      return valuePostingPartition;
    }

    public void setValuePostingPartition(int valuePostingPartition) {
      this.valuePostingPartition = valuePostingPartition;
    }

    public int getValuePostingPartitionOffset() {
      return valuePostingPartitionOffset;
    }

    public void setValuePostingPartitionOffset(int valuePostingPartitionOffset) {
      this.valuePostingPartitionOffset = valuePostingPartitionOffset;
    }

    public int getBytesLength() {
      return bytesLength;
    }

    public void setBytesLength(int bytesLength) {
      this.bytesLength = bytesLength;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
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

    @Override
    public String toString() {
      return "Posting{" +
          "flag=" + flag +
          ", createdRevision=" + createdRevision +
          ", nextKeyPostingPartition=" + nextKeyPostingPartition +
          ", nextKeyPostingPartitionOffset=" + nextKeyPostingPartitionOffset +
          ", keyHashCode=" + keyHashCode +
          ", valuePostingPartition=" + valuePostingPartition +
          ", valuePostingPartitionOffset=" + valuePostingPartitionOffset +
          ", bytesLength=" + bytesLength +
          ", bytes=" + (bytes == null ? null : Arrays.asList(bytes)) +
          ", deletedRevision=" + deletedRevision +
          '}';
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
    if (posting.flag == 0) {
      return;
    }
    posting.createdRevision = RAF.readLong();
    posting.nextKeyPostingPartition = RAF.readInt();
    posting.nextKeyPostingPartitionOffset = RAF.readInt();
    posting.keyHashCode = RAF.readLong();
    posting.valuePostingPartition = RAF.readInt();
    posting.valuePostingPartitionOffset = RAF.readInt();
    posting.bytesLength = RAF.readInt();
    if (posting.bytesLength > 0) {
      if (posting.bytes == null || posting.bytes.length != posting.bytesLength) {
        posting.bytes = new byte[posting.bytesLength];
      }
      int read = RAF.read(posting.bytes, 0, posting.bytesLength);
      if (read != posting.bytesLength) {
        throw new IOException("Unexcpected EOF");
      }
    }
    posting.deletedRevision = RAF.readLong();
  }

  public void writePosting(Posting posting, RandomAccessFile RAF) throws IOException {
    RAF.writeByte(posting.flag);
    RAF.writeLong(posting.createdRevision);
    RAF.writeInt(posting.nextKeyPostingPartition);
    RAF.writeInt(posting.nextKeyPostingPartitionOffset);
    RAF.writeLong(posting.keyHashCode);
    RAF.writeInt(posting.valuePostingPartition);
    RAF.writeInt(posting.valuePostingPartitionOffset);
    RAF.writeInt(posting.bytesLength);
    if (posting.bytesLength > 0) {
      RAF.write(posting.bytes, 0, posting.bytesLength);
    }
    RAF.writeLong(posting.deletedRevision);
  }

  public void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException {
    RAF.seek(startOffset);
    RAF.writeByte((byte) 2);
    RAF.skipBytes(8 + 4 + 4 + 8 + 4 + 4);
    RAF.skipBytes(RAF.readInt());
    RAF.writeLong(revision);
  }


}
