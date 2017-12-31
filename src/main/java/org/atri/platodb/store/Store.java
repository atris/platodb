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

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.atri.platodb.store.data.FileHandler;
import org.atri.platodb.store.data.FileHandler.Posting;
import org.atri.platodb.store.data.Metadata;
import org.atri.platodb.store.data.platotrie.HashCodesPartition;
import org.atri.platodb.store.data.platotrie.Hashtable;
import org.atri.platodb.store.data.platotrie.KeysPartition;
import org.atri.platodb.store.data.platotrie.ValuesPartition;
import org.atri.platodb.store.lock.Lock;
import org.atri.platodb.exceptions.DatabaseException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * This is the core Index<byte[], byte[]> that is stored on filesystem.
 *
 * @author atri
 * @see org.atri.platodb.entity.EntityStore
 *      <p/>
 *      todo make sure deleted postings are the last postings in all hash code chains!
 *      <p/>
 **/
public class Store {

  private static final Log log = new Log(Store.class);

  private Configuration configuration;

  private GenericObjectPool accessorPool;

  public Store(File dataPath) throws IOException {
    this(new Configuration(dataPath));
  }

  public Store(Configuration configuration) {
    this.configuration = configuration;
  }

  public void open() throws IOException {
    if (!getConfiguration().getDataPath().exists()) {
      if (log.isInfo()) {
        log.info("Creating directory " + getConfiguration().getDataPath().getAbsolutePath());
      }
      if (!getConfiguration().getDataPath().mkdirs()) {
        throw new IOException("Could not create directory " + getConfiguration().getDataPath().getAbsolutePath());
      }
    }
    GenericObjectPool.Config config = new GenericObjectPool.Config();
    config.maxIdle = 2;
    config.maxActive = 20;
    config.maxWait = 1000;
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    config.minEvictableIdleTimeMillis = 30000;
    accessorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
      public Object makeObject() throws Exception {
        return new Accessor(Store.this, false);
      }

      @Override
      public void destroyObject(Object o) throws Exception {
        ((Accessor) o).close();
      }
    }, config);
  }

  public void close() throws IOException {
    log.info("Closing store..");
//    if (accessors.size() > 0) {
//      log.warn("There are " + accessors.size() + " open accessors. They will be closed.");
//    }
//    for (Accessor accessor : new ArrayList<Accessor>(accessors)) {
//      accessor.close();
//    }
    try {
      accessorPool.close();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    log.info("Store has been closed.");

  }

  public Accessor borrowAccessor() {
    try {
	if (accessorPool == null)
	    {
		this.open();
	    }
      return (Accessor) accessorPool.borrowObject();
    } catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void returnAccessor(Accessor accessor)  {
    try {
      accessorPool.returnObject(accessor);
    } catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private void validateKey(byte[] key) {
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Null key is not allowed");
    }
  }

  public byte[] get(Accessor accessor, byte[] key, long hashCode) throws IOException {
    return get(accessor, key, hashCode, Long.MAX_VALUE);
  }

  public byte[] get(Accessor accessor, byte[] key, long hashCode, long revision) throws IOException {

    validateKey(key);

    Hashtable hashtable = accessor.getHashtable();
    Hashtable.Posting hashtablePosting = new Hashtable.Posting();
    hashtable.readPosting(hashtablePosting, hashtable.calculateHashCodePostingOffset(hashCode));

    //
    // seek to the correct hash code posting
    //
    HashCodesPartition hashCodesPartition = accessor.getHashCodesPartition(hashtablePosting.getHashCodePostingPartition());
    HashCodesPartition.Posting hashCodePosting = new HashCodesPartition.Posting();
    hashCodesPartition.readPosting(hashCodePosting, hashtablePosting.getHashCodePostingPartitionOffset());
    if (hashCodePosting.getFlagForRevision(revision) != Posting.FLAG_IN_USE) {
      return null;
    }
    while (true) {

      if (hashCodePosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE
          && hashCode == hashCodePosting.getKeyHashCode()) {
        break;
      }

      if (hashCodePosting.getNextPostingPartition() < 0) {
        return null;
      }
      if (hashCodePosting.getNextPostingPartition() != hashCodesPartition.getPartitionId()) {
        hashCodesPartition = accessor.getHashCodesPartition(hashCodePosting.getNextPostingPartition());
      }
      hashCodesPartition.readPosting(hashCodePosting, hashCodePosting.getNextPostingPartitionOffset());
    }

    //
    // seek to the correct key posting
    //

    KeysPartition.Posting keyPosting = new KeysPartition.Posting();

    KeysPartition keysPartition = accessor.getKeysPartition(hashCodePosting.getFirstKeyPostingPartition(), true);
    keysPartition.readPosting(keyPosting, hashCodePosting.getFirstKeyPostingPartitionOffset());
    while (true) {

      if (keyPosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE
          && Arrays.equals(key, keyPosting.getBytes())) {
        break;
      }

      if (keyPosting.getNextKeyPostingPartition() < 0) {
        return null;
      }
      if (keyPosting.getNextKeyPostingPartition() != keysPartition.getPartitionId()) {
        keysPartition = accessor.getKeysPartition(keyPosting.getNextKeyPostingPartition(), true);
      }
      keysPartition.readPosting(keyPosting, keyPosting.getNextKeyPostingPartitionOffset());
    }

    //
    // seek to the correct value posting
    //

    ValuesPartition.Posting valuePosting = new ValuesPartition.Posting();

    ValuesPartition valuesPartition = accessor.getValuesPartition(keyPosting.getValuePostingPartition());
    valuesPartition.readPosting(valuePosting, keyPosting.getValuePostingPartitionOffset());

    if (valuePosting.getBytesLength() == 0) {
      return null;
    }

    return valuePosting.getBytes();
  }


  /**
   * Write locking.
   *
   * @param accessor
   * @param key
   * @param hashCode
   * @param value
   * @param revision
   * @return
   * @throws IOException
   */
  public byte[] put(final Accessor accessor, final byte[] key, final long hashCode, final byte[] value, final long revision) throws IOException {

    validateKey(key);

    Lock.With<byte[]> with = new Lock.With<byte[]>(accessor.getStoreWriteLock(), getConfiguration().getLockWaitTimeoutMilliseconds()) {
      public byte[] doBody() throws IOException {
        return doPut(accessor, key, hashCode, value, revision);
      }
    };
    return with.run();
  }

  /**
   * Should be write locked at this time.
   * <p/>
   * revision must be the most recent revision in the store.
   *
   * @param accessor
   * @param key
   * @param hashCode
   * @param value
   * @return
   * @throws IOException
   */
  private byte[] doPut(final Accessor accessor, final byte[] key, final long hashCode, final byte[] value, final long revision) throws IOException {


    //
    // create new value posting
    //

    int newValuePostingPartitionNumber;
    int newValuePostingPartitionOffset;

    ValuesPartition.Posting valuePosting = new ValuesPartition.Posting();
    valuePosting.setFlag(FileHandler.Posting.FLAG_IN_USE);

    valuePosting.setCreatedRevision(revision);
    if (value == null || value.length == 0) {
      valuePosting.setBytesLength(0);
      valuePosting.setBytes(null);

      newValuePostingPartitionNumber = -1;
      newValuePostingPartitionOffset = -1;

    } else {
      valuePosting.setBytesLength(value.length);
      valuePosting.setBytes(value);

      Accessor.RequestPartitionWriterResponse<ValuesPartition> valueReservation = accessor.requestValueWrite(valuePosting);
      newValuePostingPartitionNumber = valueReservation.getFileHandler().getPartitionId();
      ValuesPartition valuesPartition = valueReservation.getFileHandler();
      newValuePostingPartitionOffset = valueReservation.getStartOffset();

      // write new value posting, keep track of partition and offset
      valuesPartition.writePosting(valuePosting, newValuePostingPartitionOffset);

    }


    //
    // create new key posting
    //

    KeysPartition.Posting newKeyPosting = new KeysPartition.Posting();
    newKeyPosting.setCreatedRevision(revision);
    newKeyPosting.setFlag(Posting.FLAG_IN_USE);
    newKeyPosting.setBytes(key);
    newKeyPosting.setBytesLength(key.length);
    newKeyPosting.setKeyHashCode(hashCode);
    newKeyPosting.setNextKeyPostingPartition(-1);
    newKeyPosting.setNextKeyPostingPartitionOffset(-1);
    newKeyPosting.setValuePostingPartition(newValuePostingPartitionNumber);
    newKeyPosting.setValuePostingPartitionOffset(newValuePostingPartitionOffset);

    Accessor.RequestPartitionWriterResponse<KeysPartition> keyReservation = accessor.requestValueWrite(newKeyPosting);
    int newKeyPostingPartitionNumber = keyReservation.getFileHandler().getPartitionId();
    KeysPartition newKeyPostingPartition = keyReservation.getFileHandler();
    int newKeyPostingPartitionOffset = keyReservation.getStartOffset();

    // note that the key posting is written to disk later later as it might need updates
    // due to durable posting links!


    //
    // find hashcode posting and hashtable posting for the new key
    //

    Hashtable hashtable = accessor.getHashtable();
    Hashtable.Posting hashtablePosting = new Hashtable.Posting();

    HashCodesPartition.Posting hashCodePosting = new HashCodesPartition.Posting();

    int hashtablePostingOffset = hashtable.calculateHashCodePostingOffset(hashCode);
    hashtable.getRAF().seek(hashtablePostingOffset);
    byte flag = hashtable.getRAF().readByte();
    if (flag == Posting.FLAG_NEVER_USED) {

      // this is the first time we create a posting at this hashtable position
      // that means there is no hash code posting either

      newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);


      hashCodePosting.setCreatedRevision(revision);
      hashCodePosting.setFlag(Posting.FLAG_IN_USE);
      hashCodePosting.setFirstKeyPostingPartition(newKeyPostingPartitionNumber);
      hashCodePosting.setFirstKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
      hashCodePosting.setKeyHashCode(hashCode);
      hashCodePosting.setNextPostingPartition(-1);
      hashCodePosting.setNextPostingPartitionOffset(-1);

      Accessor.RequestPartitionWriterResponse<HashCodesPartition> hashCodeReservation = accessor.requestValueWrite(hashCodePosting);
      int newHashCodePostingPatition = hashCodeReservation.getFileHandler().getPartitionId();
      HashCodesPartition hashCodesPartition = hashCodeReservation.getFileHandler();
      int newHashCodePostingPatitionOffset = hashCodeReservation.getStartOffset();

      hashCodesPartition.writePosting(hashCodePosting, newHashCodePostingPatitionOffset);

      // update hashtable posting

      hashtablePosting.setFlag(Posting.FLAG_IN_USE);
      hashtablePosting.setHashCodePostingPartition(newHashCodePostingPatition);
      hashtablePosting.setHashCodePostingPartitionOffset(newHashCodePostingPatitionOffset);
      hashtablePosting.setCreatedRevision(revision);
      hashtable.writePosting(hashtablePosting, hashtablePostingOffset);


      return null;

    } else {

      // there is a hashtable posting at the position for this hash code

      hashtable.readPosting(hashtablePosting, hashtablePostingOffset);

      //
      // seek to the correct hash code posting
      //      

      HashCodesPartition hashCodesPartition = accessor.getHashCodesPartition(hashtablePosting.getHashCodePostingPartition());
      int currentHashCodesPostingPartitionOffset = hashtablePosting.getHashCodePostingPartitionOffset();
      hashCodesPartition.readPosting(hashCodePosting, hashtablePosting.getHashCodePostingPartitionOffset());
      while (true) {

        if (hashCodePosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE
            && hashCode == hashCodePosting.getKeyHashCode()) {
          break;
        }

        if (hashCodePosting.getNextPostingPartition() < 0) {

          // there is no hash code posting matching this hash code.

          newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);


          //
          //  create new hash code posting
          //

          HashCodesPartition.Posting newHashCodePosting = new HashCodesPartition.Posting();
          newHashCodePosting.setCreatedRevision(revision);
          newHashCodePosting.setFlag((byte) 1);
          newHashCodePosting.setKeyHashCode(hashCode);
          newHashCodePosting.setNextPostingPartition(-1);
          newHashCodePosting.setNextPostingPartitionOffset(-1);
          newHashCodePosting.setFirstKeyPostingPartition(newKeyPostingPartitionNumber);
          newHashCodePosting.setFirstKeyPostingPartitionOffset(newKeyPostingPartitionOffset);

          Accessor.RequestPartitionWriterResponse<HashCodesPartition> hashCodeReservation = accessor.requestValueWrite(newHashCodePosting);

          int newHashCodePostingPartition = hashCodeReservation.getFileHandler().getPartitionId();
          int newHashCodePostingPartitionOffset = hashCodeReservation.getStartOffset();

          hashCodeReservation.getFileHandler().writePosting(newHashCodePosting, newHashCodePostingPartitionOffset);


          //
          // and update the current posting to point at the new one as next in chain.
          //

          hashCodePosting.setNextPostingPartition(newHashCodePostingPartition);
          hashCodePosting.setNextPostingPartitionOffset(newHashCodePostingPartitionOffset);
          hashCodesPartition.writePosting(hashCodePosting, currentHashCodesPostingPartitionOffset);

          return null;

        }

        // keep seeking..
        if (hashCodePosting.getNextPostingPartition() != hashCodesPartition.getPartitionId()) {
          hashCodesPartition = accessor.getHashCodesPartition(hashCodePosting.getNextPostingPartition());
        }
        currentHashCodesPostingPartitionOffset = hashCodePosting.getNextPostingPartitionOffset();
        hashCodesPartition.readPosting(hashCodePosting, hashCodePosting.getNextPostingPartitionOffset());
      }

      //
      // seek for the same key
      //

      KeysPartition currentKeyPostingPartition = accessor.getKeysPartition(hashCodePosting.getFirstKeyPostingPartition(), true);
      KeysPartition previousKeyPostingPartition = null;

      KeysPartition.Posting currentKeyPosting = new KeysPartition.Posting();
      KeysPartition.Posting previousKeyPosting = new KeysPartition.Posting();


      int previousKeyPostingPartitionOffset = -1;

      int currentKeyPostingPartitionOffset = hashCodePosting.getFirstKeyPostingPartitionOffset();
      currentKeyPostingPartition.readPosting(currentKeyPosting, hashCodePosting.getFirstKeyPostingPartitionOffset());
      while (true) {

        if (currentKeyPosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE
            && Arrays.equals(key, currentKeyPosting.getBytes())) {
          break;
        }
        if (currentKeyPosting.getNextKeyPostingPartition() < 0) {

          // the key did not exist
          // update the current key posting to point at the new key posting partition and offset as next in chain.
          currentKeyPosting.setNextKeyPostingPartition(newKeyPostingPartitionNumber);
          currentKeyPosting.setNextKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
          currentKeyPostingPartition.writePosting(currentKeyPosting, currentKeyPostingPartitionOffset);

          newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);

          return null;
        }

        if (currentKeyPosting.getNextKeyPostingPartition() != currentKeyPostingPartition.getPartitionId()) {
          currentKeyPostingPartition = accessor.getKeysPartition(currentKeyPosting.getNextKeyPostingPartition(), true);
        }

        previousKeyPostingPartition = currentKeyPostingPartition;
        previousKeyPostingPartitionOffset = currentKeyPostingPartitionOffset;

        currentKeyPostingPartitionOffset = currentKeyPosting.getNextKeyPostingPartitionOffset();

        currentKeyPostingPartition.readPosting(previousKeyPosting, currentKeyPosting.getNextKeyPostingPartitionOffset());
        KeysPartition.Posting tmp = previousKeyPosting;
        previousKeyPosting = currentKeyPosting;
        currentKeyPosting = tmp;
      }

      //
      // a posting exists for this key.
      //


      if (previousKeyPostingPartition == null) {
        // chain of keys with same hash code contains only two links

        // write new key posting that nothing yet points at
        // but make that new posting point at the key link we are replacing
        if (configuration.isUsingDurablePostingLinks()) {
          newKeyPosting.setNextKeyPostingPartition(currentKeyPostingPartition.getPartitionId());
          newKeyPosting.setNextKeyPostingPartitionOffset(currentKeyPostingPartitionOffset);
        }
        newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);


        // making the replaced link last of the two.
        // [nk] [ck deleted]
        currentKeyPosting.setFlag(Posting.FLAG_DELETED);
        currentKeyPosting.setDeletedRevision(revision);
	//assert currentKeyPosting.getNextKeyPostingPartition() == -1;
	//assert currentKeyPosting.getNextKeyPostingPartitionOffset() == -1;
        currentKeyPostingPartition.writePosting(currentKeyPosting, currentKeyPostingPartitionOffset);

        if (hashCodePosting.getFirstKeyPostingPartition() == currentKeyPostingPartition.getPartitionId()
            && hashCodePosting.getFirstKeyPostingPartitionOffset() == currentKeyPostingPartitionOffset) {
          // update the hash code posting
          // to point at new key posting as the first in chain
          //  [hash code posting] ---first key link+-> [nk] [ck]
          // if it used to be the current key that was the first.
          hashCodePosting.setFirstKeyPostingPartition(newKeyPostingPartitionNumber);
          hashCodePosting.setFirstKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
          hashCodesPartition.writePosting(hashCodePosting, currentHashCodesPostingPartitionOffset);
        }

      } else {

        // there are at least three links in the chain of keys with the same hash code

        if (configuration.isUsingDurablePostingLinks()) {

          // todo this is sort of ugly code

          // we have
          // [pk] [ck]

          // and now we replace current key with new key

          // [pk] [nk] [ck (deleted)]

          // new points at the current to be deleted
          newKeyPosting.setNextKeyPostingPartition(currentKeyPostingPartition.getPartitionId());
          newKeyPosting.setNextKeyPostingPartitionOffset(currentHashCodesPostingPartitionOffset);

          // previous points at new
          previousKeyPosting.setNextKeyPostingPartition(newKeyPostingPartition.getPartitionId());
          previousKeyPosting.setNextKeyPostingPartitionOffset(newKeyPostingPartitionOffset);

          // deleted points at whatever it used to point at.
          currentKeyPosting.setFlag(Posting.FLAG_DELETED);
          currentKeyPosting.setDeletedRevision(revision);

          // if first of three is deleted and it is the first of links in the whole chain
          // then make new key point at previous key and previous key back at current key
          // and point at new key as first key hash code posting link
          //
          // i.e. from:
          //  [hash code posting] ---first key link+-> [pk deleted] [nk] [ck deleted]
          // to:
          //  [hash code posting] ---first key link+-> [nk] [pk deleted] [ck deleted]
          //
          if (previousKeyPosting.getFlag() == Posting.FLAG_DELETED
              && hashCodePosting.getFirstKeyPostingPartition() == previousKeyPostingPartition.getPartitionId()
              && hashCodePosting.getFirstKeyPostingPartitionOffset() == previousKeyPostingPartitionOffset) {

            // write new key
            newKeyPosting.setNextKeyPostingPartition(previousKeyPostingPartition.getPartitionId());
            newKeyPosting.setNextKeyPostingPartitionOffset(previousKeyPostingPartitionOffset);
            newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);

            // todo short possible glitch here where old key is deleted and new key is not available
            currentKeyPostingPartition.markPostingAsDeleted(currentHashCodesPostingPartitionOffset, revision);

            // update hash code posting to point at new key posting
            hashCodePosting.setFirstKeyPostingPartition(newKeyPostingPartitionNumber);
            hashCodePosting.setFirstKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
            hashCodesPartition.writePosting(hashCodePosting, hashtablePostingOffset);

          } else {

            // write the values
            newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);
            currentKeyPostingPartition.writePosting(currentKeyPosting, currentKeyPostingPartitionOffset);
            previousKeyPostingPartition.writePosting(previousKeyPosting, previousKeyPostingPartitionOffset);
          }

        } else {

          // no durable postings


          if (previousKeyPosting.getFlag() == Posting.FLAG_DELETED
              && hashCodePosting.getFirstKeyPostingPartition() == previousKeyPostingPartition.getPartitionId()
              && hashCodePosting.getFirstKeyPostingPartitionOffset() == previousKeyPostingPartitionOffset) {

            // if the previous key is the deleted
            // and the first in chain of same hash code
            // then make the new key the first link in chain, followed by the previous
            // by updating the hash code posting

            // new points at previous
            // [nk] [pk] [ck]
            newKeyPosting.setNextKeyPostingPartition(previousKeyPostingPartition.getPartitionId());
            newKeyPosting.setNextKeyPostingPartitionOffset(previousKeyPostingPartitionOffset);
            newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);


            // update hash code
            // [hash code posting] ---first key link+-> [pk]
            // to
            // [hash code posting] ---first key link+-> [nk]
            hashCodePosting.setFirstKeyPostingPartition(newKeyPostingPartitionNumber);
            hashCodePosting.setFirstKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
            hashCodesPartition.writePosting(hashCodePosting, currentHashCodesPostingPartitionOffset);

            // mark old key as deleted
            // [nk] [pk] [ck deleted]
            currentKeyPostingPartition.markPostingAsDeleted(currentKeyPostingPartitionOffset, revision);

            // [pk] disconnects from [ck deleted]
            previousKeyPosting.setNextKeyPostingPartitionOffset(-1);
            previousKeyPosting.setNextKeyPostingPartitionOffset(-1);
            previousKeyPostingPartition.writePosting(previousKeyPosting, previousKeyPostingPartitionOffset);

          } else {

            // we have [pk] [ck]


            // create new key posting
            // and make it point at what ever [ck] points at
            newKeyPosting.setNextKeyPostingPartition(currentKeyPosting.getNextKeyPostingPartition());
            newKeyPosting.setNextKeyPostingPartitionOffset(currentKeyPosting.getNextKeyPostingPartitionOffset());
            newKeyPostingPartition.writePosting(newKeyPosting, newKeyPostingPartitionOffset);

            // replace current key as link from previous key with new key
            // [pk] [nk]
            previousKeyPosting.setNextKeyPostingPartition(newKeyPostingPartitionNumber);
            previousKeyPosting.setNextKeyPostingPartitionOffset(newKeyPostingPartitionOffset);
            previousKeyPostingPartition.writePosting(previousKeyPosting, previousKeyPostingPartitionOffset);

            // mark old disconnected key as deleted
            // [ck deleted]
            currentKeyPostingPartition.markPostingAsDeleted(currentKeyPostingPartitionOffset, revision);


          }
        }
      }


      // read the old value
      ValuesPartition valuesPartition = accessor.getValuesPartition(currentKeyPosting.getValuePostingPartition());
      valuesPartition.readPosting(valuePosting, currentKeyPosting.getValuePostingPartitionOffset());
      byte[] oldValue;
      if (valuePosting.getBytesLength() > 0) {
        oldValue = valuePosting.getBytes();
      } else {
        oldValue = null;
      }

      // mark the old value as deleted
      valuesPartition.markPostingAsDeleted(currentKeyPosting.getValuePostingPartitionOffset(), revision);

      return oldValue;

    }

  }

  /**
   * Write locking.
   *
   * @param accessor
   * @param key
   * @param hashCode
   * @param revision revision flaggad as deletion revision
   * @return
   * @throws IOException
   */
  public byte[] remove(final Accessor accessor, final byte[] key, final long hashCode, final long revision) throws IOException {

    validateKey(key);

    Lock.With<byte[]> with = new Lock.With<byte[]>(accessor.getStoreWriteLock(), getConfiguration().getLockWaitTimeoutMilliseconds()) {
      public byte[] doBody() throws IOException {

        return doRemove(accessor, key, hashCode, revision);

      }
    };
    return with.run();
  }

  /**
   * Marks all postings related with a key and its value as deleted using a specific revision.
   * <p/>
   * Should be write locked at this point!
   * <p/>
   * The code is written to handle only the current revision of the store as deletion revision,
   * it might however work with earlier revisions too.
   *
   * @param accessor
   * @param key
   * @param hashCode
   * @param revision
   * @return
   * @throws IOException
   */
  private byte[] doRemove(final Accessor accessor, final byte[] key, final long hashCode, final long revision) throws IOException {

    Hashtable.Posting hashtablePosting = new Hashtable.Posting();

    //
    // find hashtable posting
    //

    Hashtable hashtable = accessor.getHashtable();
    int hashtablePostingOffset = hashtable.calculateHashCodePostingOffset(hashCode);
    accessor.getHashtable().readPosting(hashtablePosting, hashtablePostingOffset);

    byte hashtableFlagForRevision = hashtablePosting.getFlagForRevision(revision);
    if (hashtableFlagForRevision != FileHandler.Posting.FLAG_IN_USE) {
      throw new NoSuchElementException();
    }

    if (hashtablePosting.getHashCodePostingPartition() < 0) {
      throw new StoreInconsistencyException("There is a hash table posting in the store at offset " + hashtablePostingOffset + ", hash code " + hashCode + ", but there is no pointer to a hash codes partition!");
    }


    //
    // seek to the correct hash code posting
    //

    HashCodesPartition.Posting hashCodePosting = new HashCodesPartition.Posting();

    HashCodesPartition hashCodesPartition = accessor.getHashCodesPartition(hashtablePosting.getHashCodePostingPartition());
    int currentHashCodePostingPartitionOffset = hashtablePosting.getHashCodePostingPartitionOffset();
    int currentHashCodePostingPartition = hashtablePosting.getHashCodePostingPartition();
    hashCodesPartition.readPosting(hashCodePosting, hashtablePosting.getHashCodePostingPartitionOffset());
    while (hashCode != hashCodePosting.getKeyHashCode()) {
      if (hashCodePosting.getNextPostingPartition() < 0) {
        // no hash code link postings in chain
        throw new NoSuchElementException();
      }

      if (hashCodePosting.getNextPostingPartition() != hashCodesPartition.getPartitionId()) {
        hashCodesPartition = accessor.getHashCodesPartition(hashCodePosting.getNextPostingPartition());
      }

      currentHashCodePostingPartitionOffset = hashCodePosting.getNextPostingPartitionOffset();
      currentHashCodePostingPartition = hashCodePosting.getNextPostingPartition();
      hashCodesPartition.readPosting(hashCodePosting, hashCodePosting.getNextPostingPartitionOffset());
    }

    //
    // seek for the same key
    //


    KeysPartition currentKeyLinkPostingPartition = accessor.getKeysPartition(hashCodePosting.getFirstKeyPostingPartition(), true);
    KeysPartition.Posting currentKeyLinkPosting = new KeysPartition.Posting();
    int currentKeyPostingPartitionOffset = hashCodePosting.getFirstKeyPostingPartitionOffset();

    KeysPartition previousKeyPostingPartition = null;
    KeysPartition.Posting previousKeyLinkPosting = new KeysPartition.Posting();
    int previousKeyPostingPartitionOffset = -1;

    currentKeyLinkPostingPartition.readPosting(currentKeyLinkPosting, hashCodePosting.getFirstKeyPostingPartitionOffset());
    while (true) {

      byte flagForKeyInRevision = currentKeyLinkPosting.getFlagForRevision(revision);

      if (flagForKeyInRevision == Posting.FLAG_IN_USE
          && Arrays.equals(key, currentKeyLinkPosting.getBytes())) {
        break;
      }

      if (currentKeyLinkPosting.getNextKeyPostingPartition() < 0) {
        // no more key link postings in chain
        throw new NoSuchElementException();
      }

      if (currentKeyLinkPosting.getNextKeyPostingPartition() != currentKeyLinkPostingPartition.getPartitionId()) {
        currentKeyLinkPostingPartition = accessor.getKeysPartition(currentKeyLinkPosting.getNextKeyPostingPartition(), true);
      }
      previousKeyPostingPartition = currentKeyLinkPostingPartition;
      previousKeyPostingPartitionOffset = currentKeyPostingPartitionOffset;

      currentKeyPostingPartitionOffset = currentKeyLinkPosting.getNextKeyPostingPartitionOffset();

      KeysPartition.Posting tmp = currentKeyLinkPosting;
      currentKeyLinkPostingPartition.readPosting(previousKeyLinkPosting, currentKeyLinkPosting.getNextKeyPostingPartitionOffset());
      currentKeyLinkPosting = previousKeyLinkPosting;
      previousKeyLinkPosting = tmp;

    }

    //
    // a posting exists for this key.
    //


    // process key chain
    //
    if (hashCodePosting.getFirstKeyPostingPartition() == currentKeyLinkPostingPartition.getPartitionId()
        && hashCodePosting.getFirstKeyPostingPartitionOffset() == currentKeyPostingPartitionOffset) {

      //
      // no previous key in chain
      //

      if (hashtablePosting.getHashCodePostingPartition() == currentHashCodePostingPartition
          && hashtablePosting.getHashCodePostingPartitionOffset() == currentHashCodePostingPartitionOffset) {

        //
        // no previous hash code posting in chain
        // i.e. there are now no postings in the hashtable that match the hash code of this key
        // so delete the hashtable and hash code posting
        //

        hashCodesPartition.markPostingAsDeleted(currentHashCodePostingPartitionOffset, revision);

        hashtable.markPostingAsDeleted(hashtablePostingOffset, revision);

      }

    } else {

      //
      // there is a previous key in the chain.
      // update it to point at the next key in chain as defined by the current key
      //

      if (configuration.isUsingDurablePostingLinks()) {

        //
        // place deleted posting in end of key link chain for this hash code
        //

        KeysPartition.Posting lastKeyLinkPosting = new KeysPartition.Posting();
        int lastKeyLinkPartitionPostingOffset = -1;
        // seek..
        lastKeyLinkPosting.setNextKeyPostingPartition(currentKeyLinkPostingPartition.getPartitionId());
        lastKeyLinkPosting.setNextKeyPostingPartitionOffset(currentKeyPostingPartitionOffset);
        KeysPartition lastKeyLinkPartition;
        while (true) {
          lastKeyLinkPartitionPostingOffset = lastKeyLinkPosting.getNextKeyPostingPartitionOffset();
          lastKeyLinkPartition = accessor.getKeysPartition(lastKeyLinkPosting.getNextKeyPostingPartition(), true);
          lastKeyLinkPartition.readPosting(lastKeyLinkPosting, lastKeyLinkPosting.getNextKeyPostingPartitionOffset());
          if (lastKeyLinkPosting.getNextKeyPostingPartition() < 0) {
            break;
          }
        }

        lastKeyLinkPosting.setNextKeyPostingPartition(currentKeyLinkPostingPartition.getPartitionId());
        lastKeyLinkPosting.setNextKeyPostingPartitionOffset(currentHashCodePostingPartitionOffset);
        lastKeyLinkPartition.writePosting(lastKeyLinkPosting, lastKeyLinkPartitionPostingOffset);

        currentKeyLinkPosting.setNextKeyPostingPartition(-1);
        currentKeyLinkPosting.setNextKeyPostingPartitionOffset(-1);
        currentKeyLinkPostingPartition.writePosting(currentKeyLinkPosting, currentHashCodePostingPartitionOffset);

        // todo handle internal break downs, attempt rollbacks et c
      }


      //
      // link the previous posting with what ever the deleted posting was linked to.
      //

      if (previousKeyPostingPartition != null) {
        previousKeyLinkPosting.setNextKeyPostingPartition(currentKeyLinkPosting.getNextKeyPostingPartition());
        previousKeyLinkPosting.setNextKeyPostingPartitionOffset(currentKeyLinkPosting.getNextKeyPostingPartitionOffset());
        previousKeyPostingPartition.writePosting(previousKeyLinkPosting, previousKeyPostingPartitionOffset);
      }

    }


    // mark old key as deleted
    currentKeyLinkPostingPartition.markPostingAsDeleted(currentKeyPostingPartitionOffset, revision);


    // read the old value
    ValuesPartition.Posting valuePosting = new ValuesPartition.Posting();

    ValuesPartition oldValuePartition = accessor.getValuesPartition(currentKeyLinkPosting.getValuePostingPartition());
    oldValuePartition.readPosting(valuePosting, currentKeyLinkPosting.getValuePostingPartitionOffset());
    byte[] oldValue;
    if (valuePosting.getBytesLength() > 0) {
      oldValue = valuePosting.getBytes();
    } else {
      oldValue = null;
    }

    // mark the old value as deleted
    oldValuePartition.markPostingAsDeleted(currentKeyLinkPosting.getValuePostingPartitionOffset(), revision);

    return oldValue;

  }

  public boolean containsKey(Accessor accessor, byte[] key, long hashCode) throws IOException {
    return containsKey(accessor, key, hashCode, Long.MAX_VALUE);
  }

  public boolean containsKey(Accessor accessor, byte[] key, long hashCode, long revision) throws IOException {

    validateKey(key);

    Hashtable hashtable = accessor.getHashtable();
    Hashtable.Posting hashtablePosting = new Hashtable.Posting();
    hashtable.readPosting(hashtablePosting, hashtable.calculateHashCodePostingOffset(hashCode));

    //
    // seek to the correct hash code posting
    //

    HashCodesPartition.Posting hashCodePosting = new HashCodesPartition.Posting();

    HashCodesPartition hashCodesPartition = accessor.getHashCodesPartition(hashtablePosting.getHashCodePostingPartition());
    hashCodesPartition.readPosting(hashCodePosting, hashtablePosting.getHashCodePostingPartitionOffset());


    if (hashCodePosting.getFlag() == Posting.FLAG_NEVER_USED) {
      return false;
    }

    while (true) {

      if (hashCode == hashCodePosting.getKeyHashCode()
          && hashCodePosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE) {
        break;
      }

      if (hashCodePosting.getNextPostingPartition() < 0) {
        // no more hash code links in chain
        return false;
      }
      if (hashCodePosting.getNextPostingPartition() != hashCodesPartition.getPartitionId()) {
        hashCodesPartition = accessor.getHashCodesPartition(hashCodePosting.getNextPostingPartition());
      }
      hashCodesPartition.readPosting(hashCodePosting, hashCodePosting.getNextPostingPartitionOffset());
    }

    //
    // seek to the correct key posting
    //

    KeysPartition.Posting keyPosting = new KeysPartition.Posting();

    KeysPartition keysPartition = accessor.getKeysPartition(hashCodePosting.getFirstKeyPostingPartition(), true);
    keysPartition.readPosting(keyPosting, hashCodePosting.getFirstKeyPostingPartitionOffset());
    while (true) {
      if (keyPosting.getFlagForRevision(revision) == Posting.FLAG_IN_USE
          && keyPosting.getCreatedRevision() <= revision
          && Arrays.equals(key, keyPosting.getBytes())) {
        break;
      }
      if (keyPosting.getNextKeyPostingPartition() < 0) {
        return false;
      }
      if (keyPosting.getNextKeyPostingPartition() != keysPartition.getPartitionId()) {
        keysPartition = accessor.getKeysPartition(keyPosting.getNextKeyPostingPartition(), true);
      }
      keysPartition.readPosting(keyPosting, keyPosting.getNextKeyPostingPartitionOffset());
    }

    return true;

  }

  public Cursor<KeysPartition.Posting> keys() {
    return new Cursor<KeysPartition.Posting>() {

      private int nextOffset = KeysPartition.HEADER_BYTE_SIZE;
      private int nextPartition = 0;

      public KeysPartition.Posting next(Accessor accessor, KeysPartition.Posting posting, long revision) throws IOException {
        while (true) {
          KeysPartition keysPartition = accessor.getKeysPartition(nextPartition, false);
          if (keysPartition == null) {
            return null;
          }
          keysPartition.readPosting(posting, nextOffset);
          nextOffset += posting.getPostingByteSize();
          if (posting.getFlag() == 0) {
            nextPartition++;
            nextOffset = KeysPartition.HEADER_BYTE_SIZE;
          } else if (posting.getCreatedRevision() <= revision && (posting.getDeletedRevision() == -1 || posting.getDeletedRevision() > revision)) {
            return posting;
          }
        }
      }
    };
  }

  /**
   * Removes all deleted postings from the store
   * Write locking.
   */
  public void optimize() {
    throw new UnsupportedOperationException();

  }

  /**
   * Rehashes the hash table file.
   *
   * @param accessor
   * @param resolution resolution in number of hashtable postings in new hashtable file.
   * @throws IOException
   */
  public void rehash(final Accessor accessor, final int resolution) throws IOException {
    Lock.With with = new Lock.With(accessor.getStoreWriteLock(), getConfiguration().getLockWaitTimeoutMilliseconds()) {
      public Object doBody() throws IOException {

        log.info("Rehashing hashtable capacity ??? -> " + resolution);

        Metadata metadata = accessor.getMetadata();
        Metadata.Header mdh = new Metadata.Header();
        metadata.readHeader(mdh);
        int topOldHashCodesPartition = mdh.getCurrentHashCodesPartition();
        mdh.setCurrentHashCodesPartition(mdh.getCurrentHashCodesPartition() + 1);
        metadata.writeHeader(mdh);

        Hashtable rehashedTable = new Hashtable(getConfiguration().getDataPath(), mdh.getCurrentHashtableId() + 1, accessor.getAccess(), getConfiguration().getLockFactory());
        rehashedTable.format((resolution * Hashtable.Posting.POSTING_BYTE_SIZE) + rehashedTable.getHeaderByteSize());
        rehashedTable.open();

        Hashtable.Header rehashedTableHeader = new Hashtable.Header();
        rehashedTableHeader.setPostingsCapacity(resolution);
        rehashedTable.writeHeader(rehashedTableHeader);

        // will data a new one!
        HashCodesPartition rehashCodesPartition = accessor.getHashCodesPartition(mdh.getCurrentHashCodesPartition());

        Hashtable.Posting rehashedTablePosting = new Hashtable.Posting();
        HashCodesPartition.Posting rehashCodePosting = new HashCodesPartition.Posting();
        HashCodesPartition.Header rehashCodeHeader = new HashCodesPartition.Header();
        rehashCodesPartition.readHeader(rehashCodeHeader);

        for (int currentOldHashCodePostingsPartitionId = 0; currentOldHashCodePostingsPartitionId <= topOldHashCodesPartition; currentOldHashCodePostingsPartitionId++) {
          HashCodesPartition currentOldHashCodesPartition = new HashCodesPartition(getConfiguration().getDataPath(), currentOldHashCodePostingsPartitionId, accessor.getAccess(), getConfiguration().getLockFactory());
          if (currentOldHashCodesPartition.exists()) {
            currentOldHashCodesPartition.open();
            HashCodesPartition.Header hcph = new HashCodesPartition.Header();
            currentOldHashCodesPartition.readHeader(hcph);

            HashCodesPartition.Posting hcpp = new HashCodesPartition.Posting();

            int hcpStartOffset = currentOldHashCodesPartition.getHeaderByteSize();
            while (hcpStartOffset < hcph.getNextPostingOffset()) {
              currentOldHashCodesPartition.readPosting(hcpp, hcpStartOffset);
              hcpStartOffset += hcpp.getPostingByteSize();

              if (hcpp.getFlag() == (byte) 2) {
                continue;
              } else if (hcpp.getFlag() == (byte) 0) {
                break;
              } else if (hcpp.getFlag() != (byte) 1) {
                log.warn("Unknown flag at offset " + hcpStartOffset + ": " + hcpp);
              }

              //
              // insert hashcode posting in rehashedtable and rehashcodes
              //
              rehash(accessor, hcpp, rehashedTable, rehashedTablePosting, rehashCodesPartition, rehashCodeHeader, rehashCodePosting);

              //
              // update rehashed codes partition header
              //
              rehashCodeHeader.setNextPostingOffset(rehashCodeHeader.getNextPostingOffset() + rehashCodePosting.getPostingByteSize());
              rehashCodeHeader.setBytesLeft(rehashCodeHeader.getBytesLeft() - rehashCodePosting.getPostingByteSize());

              // todo if there is no space for yet another rehased code in the partition, create new partition.
              if (rehashCodeHeader.getBytesLeft() < rehashCodePosting.getPostingByteSize()) {
                System.currentTimeMillis();
              }

            }

            currentOldHashCodesPartition.close();
          }
        }


        // write meta data header so the new hash table will be used
        metadata.readHeader(mdh);
        mdh.setCurrentHashtableId(rehashedTable.getVersionId());
        accessor.getMetadata().writeHeader(mdh);

        return null;
      }
    };
    with.run();
  }

  public Configuration getConfiguration() {
    return configuration;
  }


  /**
   * @param accessor
   * @param hashCodePosting      posting to add to rehashed table
   * @param rehashedtable        rehaseed table
   * @param rehashedtablePosting reusable posting
   * @param rehashCodePartition  rehashed codes partition
   * @param rehashCodeHeader     current header
   * @param rehashCodePosting    reusable posting
   * @throws IOException
   */
  private void rehash(Accessor accessor,
                      HashCodesPartition.Posting hashCodePosting,
                      Hashtable rehashedtable, Hashtable.Posting rehashedtablePosting,
                      HashCodesPartition rehashCodePartition, HashCodesPartition.Header rehashCodeHeader, HashCodesPartition.Posting rehashCodePosting) throws IOException {
    //
    // find hashcode posting and hashtable posting for the new key
    //

    int rehashedtablePostingOffset = rehashedtable.calculateHashCodePostingOffset(hashCodePosting.getKeyHashCode());
    rehashedtable.readPosting(rehashedtablePosting, rehashedtablePostingOffset);
    if (rehashedtablePosting.getFlag() != (byte) 1) {

      // this is the first time we create a posting at this hashtable position
      // that means there is no hash code posting either

      rehashCodePosting.setFlag((byte) 1);
      rehashCodePosting.setFirstKeyPostingPartition(hashCodePosting.getFirstKeyPostingPartition());
      rehashCodePosting.setFirstKeyPostingPartitionOffset(hashCodePosting.getFirstKeyPostingPartitionOffset());
      rehashCodePosting.setKeyHashCode(hashCodePosting.getKeyHashCode());
      rehashCodePosting.setNextPostingPartition(-1);
      rehashCodePosting.setNextPostingPartitionOffset(-1);
      rehashCodePosting.setCreatedRevision(hashCodePosting.getCreatedRevision());
      rehashCodePosting.setDeletedRevision(hashCodePosting.getDeletedRevision());

      rehashCodePartition.writePosting(rehashCodePosting, rehashCodeHeader.getNextPostingOffset());

      // update hashtable posting

      rehashedtablePosting.setFlag((byte) 1);
      rehashedtablePosting.setHashCodePostingPartition(rehashCodePartition.getPartitionId());
      rehashedtablePosting.setHashCodePostingPartitionOffset(rehashCodeHeader.getNextPostingOffset());
      rehashedtable.writePosting(rehashedtablePosting, rehashedtablePostingOffset);

      return;

    } else {

      // there is a hashtable posting at the position for this hash code

      //
      // seek to the correct hash code posting
      //

      HashCodesPartition currentRehashedCodesPartition = rehashCodePartition;
      int currentRehashCodesPostingPartitionOffset = rehashedtablePosting.getHashCodePostingPartitionOffset();
      rehashCodePartition.readPosting(rehashCodePosting, rehashedtablePosting.getHashCodePostingPartitionOffset());
      while (hashCodePosting.getKeyHashCode() != rehashCodePosting.getKeyHashCode()) {
        if (rehashCodePosting.getNextPostingPartition() < 0) {

          // there is no hash code posting matching this hash code.

          //
          //  create new hash code posting
          //

          HashCodesPartition.Posting newHashCodePosting = new HashCodesPartition.Posting();
          newHashCodePosting.setFlag((byte) 1);
          newHashCodePosting.setKeyHashCode(hashCodePosting.getKeyHashCode());
          newHashCodePosting.setNextPostingPartition(-1);
          newHashCodePosting.setNextPostingPartitionOffset(-1);
          newHashCodePosting.setFirstKeyPostingPartition(hashCodePosting.getFirstKeyPostingPartition());
          newHashCodePosting.setFirstKeyPostingPartitionOffset(hashCodePosting.getFirstKeyPostingPartitionOffset());

          rehashCodePartition.writePosting(newHashCodePosting, rehashCodeHeader.getNextPostingOffset());


          //
          // and update the current posting to point at the new one as next in chain.
          //

          rehashCodePosting.setNextPostingPartition(rehashCodePartition.getPartitionId());
          rehashCodePosting.setNextPostingPartitionOffset(rehashCodeHeader.getNextPostingOffset());
          currentRehashedCodesPartition.writePosting(rehashCodePosting, currentRehashCodesPostingPartitionOffset);

          return;

        }
        if (rehashCodePosting.getNextPostingPartition() != currentRehashedCodesPartition.getPartitionId()) {
          currentRehashedCodesPartition = accessor.getHashCodesPartition(rehashCodePosting.getNextPostingPartition());
        }
        currentRehashCodesPostingPartitionOffset = rehashCodePosting.getNextPostingPartitionOffset();
        currentRehashedCodesPartition.readPosting(rehashCodePosting, rehashCodePosting.getNextPostingPartitionOffset());
      }

    }


  }

}
