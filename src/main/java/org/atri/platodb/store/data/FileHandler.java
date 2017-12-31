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


import org.atri.platodb.store.lock.Lock;
import org.atri.platodb.store.lock.LockFactory;
import org.atri.platodb.store.Log;
import org.atri.platodb.store.StoreError;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * @author atri
 * @since 2017-mar-16 15:28:29
 */
public abstract class FileHandler<H extends FileHandler.Header, P extends FileHandler.Posting> {

  private static final Log log = new Log(FileHandler.class);

  private File file;
  private RandomAccessFile RAF;
  private String access;

  /** not implemented yet, future lock per file rather than system wide lock at write time */
  private Lock lock;

  protected FileHandler(File directory, int id, String suffix, String access, LockFactory lockFactory) throws IOException {
    StringBuilder sb = new StringBuilder(15);
    sb.append(String.valueOf(id));
    while (sb.length() < 8) {
      sb.insert(0, "0");
    }
    sb.append(".");
    sb.append(suffix);
    this.file = new File(directory, sb.toString());
    this.access = access;

    lock = lockFactory.makeLock(sb.toString());
  }

  public Lock getLock() {
    return lock;
  }

  public void format(long size) throws IOException {
    format(size, (byte) 0);
  }

  public boolean exists() {
    return getFile().exists();
  }

  public void format(long size, byte defaultValue) throws IOException {
    log.info("Formatting " + file.getAbsolutePath() + "..");

    long ms = System.currentTimeMillis();

    long leftToWrite = size;

    FileOutputStream fos = new FileOutputStream(file);
    int bufSize = Math.min(1024 * 1024, (int)(size / 5));
    byte[] bytes = new byte[bufSize];
    Arrays.fill(bytes, defaultValue);
    while (leftToWrite >= bytes.length) {
      fos.write(bytes);
      leftToWrite -= bytes.length;
    }
    if (leftToWrite > 0) {
      fos.write(bytes, defaultValue, (int) leftToWrite);
    }
    fos.close();

    log.info("It took " + (System.currentTimeMillis() - ms) + " milliseconds to data " + file.getAbsolutePath());
  }

  public void open() throws IOException {
    if (RAF != null) {
      throw new IOException("Already open");
    }
    this.RAF = new RandomAccessFile(file, access);
  }

  public void close() throws IOException {
    if (RAF != null) {
      RAF.close();
    }

  }

  public abstract int getHeaderByteSize();

  public File getFile() {
    return file;
  }

  public RandomAccessFile getRAF() {
    return RAF;
  }

  public static abstract class Header {
  }

  public static abstract class Posting {

    public static final byte FLAG_NEVER_USED = (byte)0;
    public static final byte FLAG_IN_USE = (byte)1;
    public static final byte FLAG_DELETED = (byte)2;

    public abstract int getPostingByteSize();
    public abstract byte getFlag();
    public abstract void setFlag(byte flag);
    public abstract long getCreatedRevision();
    public abstract void setCreatedRevision(long revision);
    public abstract long getDeletedRevision();
    public abstract void setDeletedRevision(long revision);


    /**
     * @param revision read revision
     * @return the flag for this posting in any given revision
     */
    public byte getFlagForRevision(long revision) {
      if (getFlag() == FileHandler.Posting.FLAG_NEVER_USED) {
        return FLAG_NEVER_USED;
      }

      if (getFlag() == FileHandler.Posting.FLAG_DELETED) {
        if (getCreatedRevision() > revision) {
          return FLAG_NEVER_USED;
        } else {
          if (getDeletedRevision() > revision) {
            return FLAG_IN_USE;
          } else {
            return FLAG_DELETED;
          }
        }
      }

      if (getFlag() == FileHandler.Posting.FLAG_IN_USE) {
        if (getCreatedRevision() > revision) {
          return FLAG_NEVER_USED;
        } else {
          return FLAG_IN_USE;
        }
      }

      throw new StoreError("Unhandled flag value: " + getFlag());
    }

  }

//  public void writePosting(P posting) throws IOException {
//    writePosting(posting, getRAF());
//  }

  /**
   * Marks the posting at the start offset as deleted
   * @param startOffset
   * @param revision
   * @throws IOException
   */
  public void markPostingAsDeleted(int startOffset, long revision) throws IOException {
    markPostingAsDeleted(startOffset, RAF, revision);
  }

  public abstract void markPostingAsDeleted(int startOffset, RandomAccessFile RAF, long revision) throws IOException;

  public void writePosting(P posting, int startOffset) throws IOException {
    writePosting(posting, startOffset, getRAF());
  }

  public void writePosting(P posting, int startOffset, RandomAccessFile RAF) throws IOException {
    RAF.seek(startOffset);
    writePosting(posting, RAF);
  }

  public void writePosting(P posting) throws IOException {
    writePosting(posting, getRAF());
  }

  public abstract void writePosting(P posting, RandomAccessFile RAF) throws IOException;

//  public void readPosting(P posting) throws IOException {
//    readPosting(posting, getRAF());
//  }

  public void readPosting(P posting, long startOffset) throws IOException {
    readPosting(posting, startOffset, getRAF());
  }

  public void readPosting(P posting, long startOffset, RandomAccessFile RAF) throws IOException {
    RAF.seek(startOffset);
    readPosting(posting, RAF);
  }

  public abstract void readPosting(P posting, RandomAccessFile RAF) throws IOException;


  public void writeHeader(H header) throws IOException {
    writeHeader(header, 0, getRAF());
  }

  public void writeHeader(H header, int startOffset) throws IOException {
    writeHeader(header, startOffset, getRAF());
  }

  public void writeHeader(H header, int startOffset, RandomAccessFile RAF) throws IOException {
    RAF.seek(startOffset);
    writeHeader(header, RAF);
  }

  public abstract void writeHeader(H header, RandomAccessFile RAF) throws IOException;

  public void readHeader(H header) throws IOException {
    readHeader(header, 0, RAF);
  }

  public void readHeader(H header, int startOffset) throws IOException {
    readHeader(header, startOffset, getRAF());
  }

  public void readHeader(H header, int startOffset, RandomAccessFile RAF) throws IOException {
    RAF.seek(startOffset);
    readHeader(header, RAF);
  }

  public abstract void readHeader(H header, RandomAccessFile RAF) throws IOException;
}
