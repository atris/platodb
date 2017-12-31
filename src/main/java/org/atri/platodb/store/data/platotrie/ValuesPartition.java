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
 * Values postings partition file.
 * <p/>
 * This file is NOT affected by rehashing.
 *
 * @author atri
 * @since 2017-mar-16 14:00:13
 */
public class ValuesPartition extends FileHandler<ValuesPartition.Header, ValuesPartition.Posting> {


  private int partitionId;

  public ValuesPartition(File directory, int partitionId, String access, LockFactory lockFactory) throws IOException {
    super(directory, partitionId, "v", access, lockFactory);
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
    private long deletedRevision = -1;
    

    /**
     * Length in bytes of serializaed value.
     * 0 == null
     */
    private int bytesLength;

    /**
     * Serialized value.
     */
    private byte[] bytes;


    public int getPostingByteSize() {
      return 1 + 8 + 8 + 8 + 4 + bytesLength + 8;
    }

    public byte getFlag() {
      return flag;
    }

    public void setFlag(byte flag) {
      this.flag = flag;
    }

    public int getBytesLength() {
      return bytesLength;
    }

    public void setBytesLength(int valueBytesLength) {
      this.bytesLength = valueBytesLength;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] valueBytes) {
      this.bytes = valueBytes;
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
    posting.createdRevision = RAF.readLong();
    posting.bytesLength = RAF.readInt();
    if (posting.bytesLength > 0) {
      posting.bytes = new byte[posting.bytesLength];
      int read = RAF.read(posting.bytes, 0, posting.bytesLength);
      if (read != posting.bytesLength) {
        throw new IOException("Unexpected EOF");
      }
    }
    posting.deletedRevision = RAF.readLong();
  }

  public void writePosting(Posting posting, RandomAccessFile RAF) throws IOException {
    RAF.writeByte(posting.flag);
    RAF.writeLong(posting.createdRevision);
    RAF.writeInt(posting.bytesLength);
    if (posting.bytesLength > 0) {
      RAF.write(posting.bytes, 0, posting.bytesLength);
    }
    RAF.writeLong(posting.deletedRevision);
  }

  public void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException {
    RAF.seek(startOffset);
    RAF.writeByte((byte) 2);
    RAF.skipBytes(8);
    RAF.skipBytes(RAF.readInt());
    RAF.writeLong(revision);
  }

}
