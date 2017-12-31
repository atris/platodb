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


import org.atri.platodb.store.lock.Lock;
import org.atri.platodb.store.data.platotrie.HashCodesPartition;
import org.atri.platodb.store.data.platotrie.Hashtable;
import org.atri.platodb.store.data.platotrie.KeysPartition;
import org.atri.platodb.store.data.platotrie.ValuesPartition;
import org.atri.platodb.store.data.FileHandler;
import org.atri.platodb.store.data.Metadata;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

/**
 * File system access interface.
 *
 * This class hold the write lock and the random access files.
 * It is thus <b>not thread safe</b>! 
 *
 * @author atri
 * @since 2017-mar-16 14:20:33
 */
public class Accessor {

  private static final Log log = new Log(Accessor.class);

  private Store store;
  private String access;

  private Lock storeWriteLock;

  private Metadata metadata;
  private Hashtable hashtable;
  private Map<Integer, HashCodesPartition> hashCodesPartitions = new HashMap<Integer, HashCodesPartition>();
  private Map<Integer, KeysPartition> keyPartitions = new HashMap<Integer, KeysPartition>();
  private Map<Integer, ValuesPartition> valuePartitions = new HashMap<Integer, ValuesPartition>();


  Accessor(final Store store, boolean readOnly) throws IOException {
    this.store = store;
    access = readOnly ? "r" : "rw";    
    storeWriteLock = store.getConfiguration().getLockFactory().makeLock("lock");
    metadata = new Metadata(store.getConfiguration().getDataPath(), access, store.getConfiguration().getLockFactory());

    if (metadata.getFile().exists()) {
      metadata.open();
    } else {

      long ms = System.currentTimeMillis();

      if (readOnly) {
        throw new IOException("Can not create a new store when accessor is in read only mode");
      }

      log.info("Creating new store..");

      Lock.With width = new Lock.With(storeWriteLock, store.getConfiguration().getLockWaitTimeoutMilliseconds()) {
        public Object doBody() throws IOException {
          if (!metadata.getFile().exists()) {

            metadata.format(metadata.getHeaderByteSize());
            metadata.open();
            Metadata.Header mdh = new Metadata.Header();
            mdh.setFileFormatVersion(0);
            mdh.setStoreRevision(0);
            mdh.setCurrentHashtableId(0);
            mdh.setCurrentHashCodesPartition(0);
            mdh.setCurrentKeysPartition(0);
            mdh.setValuePostingsCount(0);
            metadata.writeHeader(mdh);

            hashtable = new Hashtable(store.getConfiguration().getDataPath(), 0, access, store.getConfiguration().getLockFactory());
            hashtable.format((store.getConfiguration().getInitialCapacity() * Hashtable.Posting.POSTING_BYTE_SIZE) + hashtable.getHeaderByteSize());
            hashtable.open();
            Hashtable.Header hth = new Hashtable.Header();
            hth.setPostingsCapacity(store.getConfiguration().getInitialCapacity());
            hashtable.writeHeader(hth);

          }
          return null;
        }
      };
      width.run();

      // these could be opened lazy, but let's do it now.
      getHashCodesPartition(0);
      getKeysPartition(0, readOnly);
      getValuesPartition(0);

      ms = System.currentTimeMillis() - ms;
      log.info("New store has been created. Took " + ms + " milliseconds.");

    }
  }

  public long increaseStoreRevision() throws IOException {
    return new Lock.With<Long>(storeWriteLock, store.getConfiguration().getLockWaitTimeoutMilliseconds()) {
      public Long doBody() throws IOException {
        Metadata.Header mdh = new Metadata.Header();
        getMetadata().readHeader(mdh);
        mdh.increaseRevision(1l);
        getMetadata().writeHeader(mdh);
        return mdh.getStoreRevision();
      }
    }.run();
  }

  public void close() throws IOException {
    metadata.close();
    if (hashtable != null) {
      hashtable.close();
    }
    for (FileHandler fileHandler : hashCodesPartitions.values()) {
      fileHandler.close();
    }
    for (FileHandler fileHandler : keyPartitions.values()) {
      fileHandler.close();
    }
    for (FileHandler fileHandler : valuePartitions.values()) {
      fileHandler.close();
    }
    metadata = null;
    hashtable = null;
    hashCodesPartitions = null;
    keyPartitions = null;
    valuePartitions = null;
  }


  public Metadata getMetadata() throws IOException {
    return metadata;
  }

  public Hashtable getHashtable() throws IOException {
    Metadata.Header metadataHeader = new Metadata.Header();
    metadata.readHeader(metadataHeader);
    if (hashtable == null || metadataHeader.getCurrentHashtableId() != hashtable.getVersionId()) {
      if (hashtable != null) {
        hashtable.getRAF().close();
      }
      hashtable = new Hashtable(store.getConfiguration().getDataPath(), metadataHeader.getCurrentHashtableId(), access, store.getConfiguration().getLockFactory());
      hashtable.open();
    }
    return hashtable;
  }

  public HashCodesPartition getHashCodesPartition(int partitionId) throws IOException {
    HashCodesPartition partition = hashCodesPartitions.get(partitionId);
    if (partition == null) {
      partition = new HashCodesPartition(store.getConfiguration().getDataPath(), partitionId, access, store.getConfiguration().getLockFactory());
      if (!partition.getFile().exists()) {
        final HashCodesPartition p = partition;
        Lock.With with = new Lock.With(storeWriteLock, store.getConfiguration().getLockWaitTimeoutMilliseconds()) {
          public Object doBody() throws IOException {
            if (!p.getFile().exists()) {
              p.format(store.getConfiguration().getHashCodesPartitionByteSize());
              p.open();
              HashCodesPartition.Header hch = new HashCodesPartition.Header();
              hch.setNextPostingOffset(p.getHeaderByteSize());
              hch.setBytesLeft(store.getConfiguration().getHashCodesPartitionByteSize() - p.getHeaderByteSize());
              p.writeHeader(hch);
            }
            return null;
          }
        };
        with.run();
      } else {
        partition.open();
      }
      hashCodesPartitions.put(partitionId, partition);
    }
    return partition;
  }



  public KeysPartition getKeysPartition(int partitionId, boolean createNew) throws IOException {
    KeysPartition partition = keyPartitions.get(partitionId);
    if (partition == null) {
      partition = new KeysPartition(store.getConfiguration().getDataPath(), partitionId, access, store.getConfiguration().getLockFactory());

      if (!partition.getFile().exists() && !createNew) {
        partition.close();
        return null;
      }
      

      if (!partition.getFile().exists()) {
        final KeysPartition p = partition;
        Lock.With with = new Lock.With(storeWriteLock, store.getConfiguration().getLockWaitTimeoutMilliseconds()) {
          public Object doBody() throws IOException {
            if (!p.getFile().exists()) {
              p.format(store.getConfiguration().getKeysPartitionByteSize());
              p.open();
              KeysPartition.Header kh = new KeysPartition.Header();
              kh.setNextPostingOffset(p.getHeaderByteSize());
              kh.setBytesLeft(store.getConfiguration().getKeysPartitionByteSize() - p.getHeaderByteSize());
              p.writeHeader(kh);
            }
            return null;
          }
        };
        with.run();
      } else {
        partition.open();
      }
      keyPartitions.put(partitionId, partition);
    }
    return partition;
  }

  public ValuesPartition getValuesPartition(int partitionId) throws IOException {
    ValuesPartition partition = valuePartitions.get(partitionId);
    if (partition == null) {
      partition = new ValuesPartition(store.getConfiguration().getDataPath(), partitionId, access, store.getConfiguration().getLockFactory());
      if (!partition.getFile().exists()) {
        final ValuesPartition p = partition;
        Lock.With with = new Lock.With(storeWriteLock, store.getConfiguration().getLockWaitTimeoutMilliseconds()) {
          public Object doBody() throws IOException {
            if (!p.getFile().exists()) {
              p.format(store.getConfiguration().getValuesPartitionByteSize());
              p.open();
              ValuesPartition.Header vh = new ValuesPartition.Header();
              vh.setNextPostingOffset(p.getHeaderByteSize());
              vh.setBytesLeft(store.getConfiguration().getValuesPartitionByteSize() - p.getHeaderByteSize());
              p.writeHeader(vh);
            }
            return null;
          }
        };
        with.run();
      } else {
        partition.open();
      }
      valuePartitions.put(partitionId, partition);
    }
    return partition;
  }

  /**
   * Require write lock!
   *
   * @param posting
   * @return
   * @throws IOException
   */
  public RequestPartitionWriterResponse<ValuesPartition> requestValueWrite(ValuesPartition.Posting posting) throws IOException {

    int requestedBytes = posting.getPostingByteSize();

    Metadata.Header mdh = new Metadata.Header();
    metadata.readHeader(mdh);

    ValuesPartition vp = getValuesPartition(mdh.getCurrentValuesPartition());
    ValuesPartition.Header vph = new ValuesPartition.Header();

    vp.readHeader(vph);
    if (vph.getBytesLeft() < requestedBytes) {

      int maxPostingByteSize = store.getConfiguration().getValuesPartitionByteSize() - vp.getHeaderByteSize();
      if (requestedBytes > maxPostingByteSize) {
        throw new IOException("Value posting is too large ("+requestedBytes+" bytes) to fit the maximum values postings paritition size of "+ maxPostingByteSize +" bytes.");
      }

      mdh.setCurrentValuesPartition(mdh.getCurrentValuesPartition() + 1);
      metadata.writeHeader(mdh);

      vp = getValuesPartition(mdh.getCurrentValuesPartition());
      vp.readHeader(vph);
    }

    RequestPartitionWriterResponse<ValuesPartition> response = new RequestPartitionWriterResponse<ValuesPartition>();

    response.fileHandler = vp;
    response.startOffset = vph.getNextPostingOffset();

    vph.setBytesLeft(vph.getBytesLeft() - requestedBytes);
    vph.setNextPostingOffset(vph.getNextPostingOffset() + requestedBytes);
    vp.writeHeader(vph);

    return response;
  }

  /**
   * Require write lock!
   *
   * @param posting
   * @return
   * @throws IOException
   */
  public RequestPartitionWriterResponse<KeysPartition> requestValueWrite(KeysPartition.Posting posting) throws IOException {

    int requestedBytes = posting.getPostingByteSize();

    Metadata.Header mdh = new Metadata.Header();
    metadata.readHeader(mdh);

    KeysPartition kp = getKeysPartition(mdh.getCurrentKeysPartition(), true);
    KeysPartition.Header kh = new KeysPartition.Header();

    kp.readHeader(kh);
    if (kh.getBytesLeft() < requestedBytes) {

      int maxPostingByteSize = store.getConfiguration().getKeysPartitionByteSize() - kp.getHeaderByteSize();
      if (requestedBytes > maxPostingByteSize) {
        throw new IOException("Key posting is too large ("+requestedBytes+" bytes) to fit the maximum key postings paritition size of "+ maxPostingByteSize +" bytes.");
      }


      mdh.setCurrentKeysPartition(mdh.getCurrentKeysPartition() + 1);
      metadata.writeHeader(mdh);

      kp = getKeysPartition(mdh.getCurrentKeysPartition(), true);
      kp.readHeader(kh);
    }

    RequestPartitionWriterResponse<KeysPartition> response = new RequestPartitionWriterResponse<KeysPartition>();

    response.fileHandler = kp;
    response.startOffset = kh.getNextPostingOffset();

    kh.setBytesLeft(kh.getBytesLeft() - requestedBytes);
    kh.setNextPostingOffset(kh.getNextPostingOffset() + requestedBytes);
    kp.writeHeader(kh);

    return response;
  }

  /**
   * Require write lock!
   *
   * @param posting
   * @return
   * @throws IOException
   */
  public RequestPartitionWriterResponse<HashCodesPartition> requestValueWrite(HashCodesPartition.Posting posting) throws IOException {

    int requestedBytes = posting.getPostingByteSize();

    Metadata.Header mdh = new Metadata.Header();
    metadata.readHeader(mdh);

    HashCodesPartition hcp = getHashCodesPartition(mdh.getCurrentHashCodesPartition());
    HashCodesPartition.Header hch = new HashCodesPartition.Header();

    hcp.readHeader(hch);
    if (hch.getBytesLeft() < requestedBytes) {

      int maxPostingByteSize = store.getConfiguration().getHashCodesPartitionByteSize() - hcp.getHeaderByteSize();
      if (requestedBytes > maxPostingByteSize) {
        throw new IOException("Hash code posting is too large ("+requestedBytes+" bytes) to fit the maximum hash code postings paritition size of "+ maxPostingByteSize +" bytes.");
      }


      mdh.setCurrentHashCodesPartition(mdh.getCurrentHashCodesPartition() + 1);
      metadata.writeHeader(mdh);

      hcp = getHashCodesPartition(mdh.getCurrentHashCodesPartition());
      hcp.readHeader(hch);
    }

    RequestPartitionWriterResponse<HashCodesPartition> response = new RequestPartitionWriterResponse<HashCodesPartition>();

    response.fileHandler = hcp;
    response.startOffset = hch.getNextPostingOffset();

    hch.setBytesLeft(hch.getBytesLeft() - requestedBytes);
    hch.setNextPostingOffset(hch.getNextPostingOffset() + requestedBytes);
    hcp.writeHeader(hch);

    return response;
  }

  public static class RequestPartitionWriterResponse<T extends FileHandler> {
    private T fileHandler;
    private int startOffset;

    public T getFileHandler() {
      return fileHandler;
    }

    public int getStartOffset() {
      return startOffset;
    }
  }

  public Lock getStoreWriteLock() {
    return storeWriteLock;
  }

  public Store getStore() {
    return store;
  }

  public String getAccess() {
    return access;
  }
}
