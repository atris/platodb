package org.atri.platodb.entity;

import org.atri.platodb.entity.serialization.HashCodeCalculator;
import org.atri.platodb.entity.serialization.Marshaller;
import org.atri.platodb.entity.serialization.Unmarshaller;
import org.atri.platodb.exceptions.DatabaseException;
import org.atri.platodb.store.Accessor;
import org.atri.platodb.store.Cursor;
import org.atri.platodb.store.Store;
import org.atri.platodb.store.data.platotrie.KeysPartition;
import org.atri.platodb.store.data.platotrie.ValuesPartition;
import org.atri.platodb.store.sequence.SequenceManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

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


/**
 * The consumer interface for reading and writing to an entity store.
 * <p/>
 * Each primary index is backed by its own {@link org.atri.platodb.store.Store} instance.
 *
 * @see org.atri.platodb.entity.EntityStore#getPrimaryIndex(Class, Class, String)
 */
public class PrimaryIndex<K, E> {

  private String indexName;
  private Store store;

  /**
   * holds the thread local transaction
   */
  private EntityStore entityStore;

  private Marshaller keyMarshaller;
  private Unmarshaller keyUnmarshaller;
  private HashCodeCalculator keyHashCodeCalculator;
  private Marshaller entityMarshaller;
  private Unmarshaller entityUnmarshaller;

  private Class<K> keyClass;
  private Class<E> entityClass;

  private Method primaryKeyGetter;
  private Method primaryKeySetter;

  private SequenceManager.Sequence<K> primaryKeySequence;

  /**
   * @param store
   * @param entityStore
   * @param indexName
   * @param primaryKeySequence
   * @param primaryKeyGetter
   * @param primaryKeySetter
   * @param keyClass
   * @param entityClass
   * @param keyMarshaller
   * @param keyUnmarshaller
   * @param keyHashCodeCalculator
   * @param entityMarshaller
   * @param entityUnmarshaller
   * @see org.atri.platodb.entity.EntityStore#getPrimaryIndex(Class, Class, String)
   */
  PrimaryIndex(Store store, EntityStore entityStore, String indexName, SequenceManager.Sequence<K> primaryKeySequence, Method primaryKeyGetter, Method primaryKeySetter, Class<K> keyClass, Class<E> entityClass, Marshaller keyMarshaller, Unmarshaller keyUnmarshaller, HashCodeCalculator keyHashCodeCalculator, Marshaller entityMarshaller, Unmarshaller entityUnmarshaller) {
    this.store = store;
    this.entityStore = entityStore;
    this.indexName = indexName;

    this.primaryKeySequence = primaryKeySequence;

    this.primaryKeyGetter = primaryKeyGetter;
    this.primaryKeySetter = primaryKeySetter;

    this.keyClass = keyClass;
    this.entityClass = entityClass;

    this.keyMarshaller = keyMarshaller;
    this.keyUnmarshaller = keyUnmarshaller;
    this.keyHashCodeCalculator = keyHashCodeCalculator;
    this.entityMarshaller = entityMarshaller;
    this.entityUnmarshaller = entityUnmarshaller;
  }


  @SuppressWarnings("unchecked")
  public E get(K key, long revision) {
//    long keyHashCode = keyHashCodeCalculator.calcualteLongHashCode(key);
//    byte[] keyBytes = keyMarshaller.marshall(key);

    try {
      long keyHashCode = calculatePrimaryIndexKeyHashCode(key);
      byte[] keyBytes = marshalPrimayIndexKey(key);

      Accessor accessor = store.borrowAccessor();
      byte[] entityBytes = store.get(accessor, keyBytes, keyHashCode, revision);
      store.returnAccessor(accessor);
      if (entityBytes == null) {
        return null;
      }
      return (E) entityUnmarshaller.unmarshall(entityBytes);
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  public boolean containsKey(final K key) {

    try {
      Transaction txn = getEntityStore().getTxn();
      if (txn.isActive()) {

        txn.getIsolation().checkVersion(txn);

        CachedKey cachedKey = new CachedKey(key);

        return !txn.getRemoved().containsKey(cachedKey)
            && (
            txn.getReplaced().containsKey(cachedKey)
                || txn.getCreated().containsKey(cachedKey)
                || containsKey(key, entityStore.getTxn().getDefaultReadRevision()));

      } else {
        return containsKey(key, entityStore.getTxn().getDefaultReadRevision());
      }
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  public boolean containsKey(K key, long revision) {
    try {
      long keyHashCode = calculatePrimaryIndexKeyHashCode(key);
      byte[] keyBytes = marshalPrimayIndexKey(key);

      Accessor accessor = store.borrowAccessor();
      boolean result = store.containsKey(accessor, keyBytes, keyHashCode, revision);
      store.returnAccessor(accessor);
      return result;
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }

  @SuppressWarnings("unchecked")
  public E put(E entity, long revision) {
    try {
      K key = getPrimaryKey(entity);
      if (key == null) {
        if (primaryKeySequence == null) {
          throw new UnsupportedOperationException("Null keys are not allowed. Did you perhaps forget to set sequence() to something in the @PrimaryKey annotation of entity class " + entityClass.getName() + "?");
        }
        key = primaryKeySequence.next();
        setPrimaryKey(entity, key);
      }
//    long keyHashCode = keyHashCodeCalculator.calcualteLongHashCode(key);
//    byte[] keyBytes = keyMarshaller.marshall(key);
      long keyHashCode = calculatePrimaryIndexKeyHashCode(key);
      byte[] keyBytes = marshalPrimayIndexKey(key);

      byte[] entityBytes = entityMarshaller.marshall(entity);

      Accessor accessor = store.borrowAccessor();
      byte[] oldEntityBytes = store.put(accessor, keyBytes, keyHashCode, entityBytes, revision);
      store.returnAccessor(accessor);

      if (oldEntityBytes == null) {
        return null;
      } else {
        return (E) entityUnmarshaller.unmarshall(oldEntityBytes);
      }
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }

  @SuppressWarnings("unchecked")
  public E remove(K key, long revision) {
    try {
//    long keyHashCode = keyHashCodeCalculator.calcualteLongHashCode(key);
//    byte[] keyBytes = keyMarshaller.marshall(key);

      long keyHashCode = calculatePrimaryIndexKeyHashCode(key);
      byte[] keyBytes = marshalPrimayIndexKey(key);


      Accessor accessor = store.borrowAccessor();
      byte[] oldEntityBytes = store.remove(accessor, keyBytes, keyHashCode, revision);
      store.returnAccessor(accessor);

      if (oldEntityBytes == null) {
        return null;
      } else {
        return (E) entityUnmarshaller.unmarshall(oldEntityBytes);
      }
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  @SuppressWarnings("unchecked")
  public K getPrimaryKey(E entity) {
    try {
      return (K) primaryKeyGetter.invoke(entity);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public void setPrimaryKey(E entity, K key) {
    try {
      primaryKeySetter.invoke(entity, key);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public EntityCursor<K, E> cursor() {
    return cursor(getEntityStore().getTxn().getDefaultReadRevision());
  }

  public EntityCursor<K, E> cursor(final long revision) {
    final Accessor accessor = store.borrowAccessor();
    final Cursor<KeysPartition.Posting> keysCursor = store.keys();
    return new EntityCursor<K, E>() {
      private KeysPartition.Posting keyPosting = new KeysPartition.Posting();
      private ValuesPartition.Posting valuePosting = new ValuesPartition.Posting();

      public boolean next() {
        try {
          boolean result = (keyPosting = keysCursor.next(accessor, keyPosting, revision)) != null;
          key = null;
          value = null;
          return result;
        } catch (IOException ioe) {
          throw new DatabaseException(ioe);
        }

      }

      private K key;

      public K key() {
        try {
          if (key == null) {
            key = unmarshalPrimaryIndexKey(keyPosting.getBytes());
          }
          return key;
        } catch (IOException ioe) {
          throw new DatabaseException(ioe);
        }

      }

      private E value;

      public E value() {
        try {
          if (value == null) {
            accessor.getValuesPartition(keyPosting.getValuePostingPartition()).readPosting(valuePosting, keyPosting.getValuePostingPartitionOffset());
            value = (E) entityUnmarshaller.unmarshall(valuePosting.getBytes());
          }
          return value;
        } catch (IOException ioe) {
          throw new DatabaseException(ioe);
        }

      }

      public void remove() {
        PrimaryIndex.this.remove(key());
      }

      public void close() {
        store.returnAccessor(accessor);
      }
    };
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<E> getEntityClass() {
    return entityClass;
  }

  public EntityStore getEntityStore() {
    return entityStore;
  }

  public SequenceManager.Sequence<K> getPrimaryKeySequence() {
    return primaryKeySequence;
  }


  // transactional

  public class CachedKey {
    private byte[] bytes;
    private int hashCode;

    private CachedKey(K key) throws IOException {
      bytes = keyMarshaller.marshall(key);
      hashCode = keyHashCodeCalculator.calcualteIntegerHashCode(key);
    }

    @SuppressWarnings("unchecked")
    public K getObject() throws IOException {
      return (K) keyUnmarshaller.unmarshall(bytes);
    }

    public int getHashCode() {
      return hashCode;
    }

    public PrimaryIndex<K, E> getPrimaryIndex() {
      return PrimaryIndex.this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CachedKey cachedKey = (CachedKey) o;

      if (hashCode != cachedKey.hashCode) return false;
      if (!Arrays.equals(bytes, cachedKey.bytes)) return false;
      if (getPrimaryIndex() != cachedKey.getPrimaryIndex()) return false;
      return true;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  public class CachedEntity {

    private byte[] bytes;

    private CachedEntity(E entity) throws IOException {
      if (entity != null) {
        bytes = entityMarshaller.marshall(entity);
      } else {
        bytes = null;
      }
    }

    @SuppressWarnings("unchecked")
    public E getObject() throws IOException {
      if (bytes == null) {
        return null;
      }
      return (E) entityUnmarshaller.unmarshall(bytes);
    }

    public PrimaryIndex<K, E> getPrimaryIndex() {
      return PrimaryIndex.this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CachedEntity that = (CachedEntity) o;

      if (!Arrays.equals(bytes, that.bytes)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return bytes != null ? Arrays.hashCode(bytes) : 0;
    }
  }

  @SuppressWarnings("unchecked")
  public E get(K key) {

    try {
      Transaction txn = getEntityStore().getTxn();
      if (txn.isActive()) {

        txn.getIsolation().checkVersion(txn);

        CachedKey cachedKey = new CachedKey(key);

        if (txn.getRemoved().containsKey(cachedKey)) {
          return null;
        }
        E v;
        if (txn.getCreated().containsKey(cachedKey)) {
          v = (E) txn.getCreated().get(cachedKey).getObject();
        } else if (txn.getReplaced().containsKey(cachedKey)) {
          v = (E) txn.getReplaced().get(cachedKey).getObject();
        } else {
          v = get(key, entityStore.getTxn().getDefaultReadRevision());
        }
        return v;
      } else {
        return get(key, entityStore.getTxn().getDefaultReadRevision());
      }

    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  @SuppressWarnings("unchecked")
  public E put(E entity) {

    try {
      Transaction txn = getEntityStore().getTxn();
      if (txn.isActive()) {

        K key = getPrimaryKey(entity);
        if (key == null) {
          setPrimaryKey(entity, key = primaryKeySequence.next());
        }


        CachedKey cachedKey = new CachedKey(key);
        CachedEntity cachedEntity = new CachedEntity(entity);

        txn.getIsolation().checkVersion(txn);

        E v;
        if (txn.getCreated().containsKey(cachedKey)) {
          v = (E) txn.getCreated().put(cachedKey, cachedEntity).getObject();
        } else if (txn.getReplaced().containsKey(cachedKey)) {
          v = (E) txn.getReplaced().put(cachedKey, cachedEntity).getObject();
        } else {
          if (containsKey(key)) {
            txn.getReplaced().put(cachedKey, cachedEntity);
            if (txn.getRemoved().containsKey(cachedKey)) {
              txn.getRemoved().remove(cachedKey);
              v = null;
            } else {
              v = get(key);
            }
          } else {
            txn.getCreated().put(cachedKey, cachedEntity);
            v = null;
            txn.getRemoved().remove(cachedKey); // todo not needed?
          }
        }
        return v;
      } else {
        // transactionless
        return put(entity, entityStore.increaseStoreRevision());
      }
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  @SuppressWarnings("unchecked")
  public E remove(final K key) {

    try {

      Transaction txn = getEntityStore().getTxn();
      if (txn.isActive()) {

        txn.getIsolation().checkVersion(txn);

        CachedKey cachedKey = new CachedKey(key);


        if (txn.getRemoved().containsKey(cachedKey)) {
          return null;
        }
        E v;
        if (txn.getCreated().containsKey(cachedKey)) {
          v = (E) txn.getCreated().remove(cachedKey).getObject();
        } else if (txn.getReplaced().containsKey(cachedKey)) {
          v = (E) txn.getReplaced().remove(cachedKey).getObject();
        } else {
          v = get(key);
          txn.getRemoved().put(cachedKey, new CachedEntity(v));
        }
        return v;
      } else {
        // transactionless
        return remove(key, entityStore.increaseStoreRevision());
      }
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }

  /**
   * @return
   * @see #count(long)
   */
  public long count() {
    return count(getEntityStore().getTxn().getDefaultReadRevision());
  }

  /**
   * Counts number of entities in the store.
   * <p/>
   * A primary index and its store is unaware of how many entities it keeps for a given revision.
   * In order to find this out it needs to skip through all items using a key cursor
   * and that can take a long time.
   * <p/>
   * Todo this does not take
   *
   * @param revision
   * @return
   * @throws IOException
   */
  public long count(long revision) {

    try {
      Accessor accessor = store.borrowAccessor();
      Cursor<KeysPartition.Posting> cursor = store.keys();
      KeysPartition.Posting posting = new KeysPartition.Posting();
      long count = 0;
      // todo implement ad hoc cursor on keys that only reads the data needed to skip by all items.
      while ((cursor.next(accessor, posting, revision)) != null) {
        count++;
      }
      store.returnAccessor(accessor);

      Transaction txn = getEntityStore().getTxn();
      if (txn.isActive()) {
        count += txn.getCreated().size();
        count += txn.getReplaced().size();
        count -= txn.getRemoved().size();
        count += txn.getIsolation().getCountModifier();
      }
      return count;
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }

//  public long size() throws IOException {
//    Metadata.Header mdh = new Metadata.Header();
//    Accessor accessor = store.borrowAccessor();
//    accessor.getMetadata().readHeader(mdh);
//    store.returnAccessor(accessor);
//    return mdh.getValuePostingsCount();
//  }
//
//    isolation.checkVersion(Transaction.this);
//
//    Lock.With<Integer> with = new Lock.With<Integer>(lock, lockWaitTimeoutMilliseconds) {
//      protected Integer doBody() throws IOException {
//        int size = entityStore.size(accessor);
//        size += created.size();
//        size += replaced.size();
//        size -= removed.size();
//        size += isolation.getSizeModifier();
//        return size;
//      }
//    };
//    return with.run();
//  }


  //


  /**
   * This method is used to bundle entity class name in the key
   * so that multiple instance of PrimaryIndex with different entity types
   * can use the same key class
   *
   * @param key
   * @return
   * @throws IOException
   */
  private byte[] marshalPrimayIndexKey(K key) throws IOException {
    byte[] keyBytes = keyMarshaller.marshall(key);
//    byte[] out = new byte[keyBytes.length + indexNameByteArray.length];
//    System.arraycopy(keyBytes, 0, out, 0, keyBytes.length);
//    System.arraycopy(indexNameByteArray, 0, out, keyBytes.length, indexNameByteArray.length);
    return keyBytes;
  }

  @SuppressWarnings("unchecked")
  private K unmarshalPrimaryIndexKey(byte[] bytes) throws IOException {
    return (K) keyUnmarshaller.unmarshall(bytes);
//    return (K) keyUnmarshaller.unmarshall(bytes, 0, bytes.length - indexNameByteArray.length);
  }

  private long calculatePrimaryIndexKeyHashCode(K key) {
    long result = 0;
//    result = indexNameHashCode << 32;
    result += keyHashCodeCalculator.calcualteIntegerHashCode(key);
    return result;
  }

  public String getIndexName() {
    return indexName;
  }

  public void close() {
    if (getPrimaryKeySequence() != null) {
      try {
        getPrimaryKeySequence().close();
      } catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }

  Store getStore() {
    return store;
  }
}
