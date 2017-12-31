package org.atri.platodb.entity;

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


import org.atri.platodb.entity.isolation.IsolationStrategy;
import org.atri.platodb.exceptions.DatabaseException;
import org.atri.platodb.store.lock.Lock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**s
 * @author atri
 * @since 2017-mar-14 13:29:29
 */
public class Transaction {

  private boolean active = false;

  public boolean isActive() {
    return active;
  }

  private IsolationStrategy isolation;

  private long storeRevisionTransactionIsSynchronizedWith = 0l;

  private Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> created;
  private Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> replaced;


  /**
   * contains the value in the hashtable when it was removed
   */
  private Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> removed;

  private EntityStore entityStore;

  public Transaction(EntityStore entityStore) {
    this.entityStore = entityStore;
    isolation = entityStore.getConfiguration().getDefaultIsolation();
  }

  private long defaultReadRevision = Long.MAX_VALUE; // latest revision for non transactional

  public void begin() {
    begin(Long.MAX_VALUE);
  }

  public synchronized void begin(long readRevision) {
    if (active) {
      throw new DatabaseException("Transaction has already begun");
    }
    active = true;
    this.defaultReadRevision = readRevision;
    created = new HashMap<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>();
    replaced = new HashMap<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>();
    removed = new HashMap<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>();

    try {
      storeRevisionTransactionIsSynchronizedWith = entityStore.getStoreRevision();
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }
  }

  /**
   * @return committed revision
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public synchronized long commit() {
    if (!active) {
      throw new DatabaseException("Transaction is not started");
    }

    try {
      return new Lock.With<Long>(entityStore.getStoreWriteLock(), entityStore.getConfiguration().getLockWaitTimeoutMilliseconds()) {
        public Long doBody() throws IOException {
          isolation.checkVersion(Transaction.this);

          long revision = entityStore.increaseStoreRevision();

          for (Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e : removed.entrySet()) {
            e.getKey().getPrimaryIndex().remove(e.getKey().getObject(), revision);
          }
          for (Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e : created.entrySet()) {
            e.getKey().getPrimaryIndex().put(e.getValue().getObject(), revision);
          }
          for (Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e : replaced.entrySet()) {
            e.getKey().getPrimaryIndex().put(e.getValue().getObject(), revision);
          }

          created = null;
          replaced = null;
          removed = null;

          active = false;

          return revision;
        }
      }.run();
    } catch (IOException ioe) {
      throw new DatabaseException(ioe);
    }

  }


  public synchronized void abort() {

    if (!active) {
      throw new DatabaseException("Transaction is not started");
    }

    created = null;
    replaced = null;
    removed = null;

    active = false;

  }


  public long getStoreRevisionTransactionIsSynchronizedWith() {
    return storeRevisionTransactionIsSynchronizedWith;
  }


  public Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> getCreated() {
    return created;
  }

  public Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> getReplaced() {
    return replaced;
  }

  public Map<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> getRemoved() {
    return removed;
  }

  public EntityStore getEntityStore() {
    return entityStore;
  }

  public void setStoreRevisionTransactionIsSynchronizedWith(long storeRevisionTransactionIsSynchronizedWith) {
    this.storeRevisionTransactionIsSynchronizedWith = storeRevisionTransactionIsSynchronizedWith;
  }

  public IsolationStrategy getIsolation() {
    return isolation;
  }

  public void setIsolation(IsolationStrategy isolation) {
    this.isolation = isolation;
  }

  public long getDefaultReadRevision() {
    return defaultReadRevision;
  }

  public void setDefaultReadRevision(long defaultReadRevision) {
    this.defaultReadRevision = defaultReadRevision;
  }
}
