package org.atri.platodb.entity.isolation;

import org.atri.platodb.entity.PrimaryIndex;
import org.atri.platodb.entity.Transaction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Replaces instances in your transaction with changes committed by someone else.
 * I.e. there is a chanse that your changes will be gone if you don't commit in time.
 *
 * @author atri
 * @since 2017-mar-14 15:40:40
 */
public class AlwaysUpdated extends AbstractIsolationStrategy {

  public void checkVersion(Transaction txn) throws IOException {

    long storeRevision = txn.getEntityStore().getStoreRevision();

    if (txn.getStoreRevisionTransactionIsSynchronizedWith() != storeRevision) {

      txn.setStoreRevisionTransactionIsSynchronizedWith(storeRevision);

      for (Iterator<Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>> it = txn.getRemoved().entrySet().iterator(); it.hasNext();) {
        Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e = it.next();
        PrimaryIndex primaryIndex = e.getKey().getPrimaryIndex();
        if (!primaryIndex.containsKey(e.getKey().getObject())) {
          it.remove();
        }
      }

      Set<PrimaryIndex.CachedKey> moved = new HashSet<PrimaryIndex.CachedKey>();
      for (Iterator<Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>> it = txn.getCreated().entrySet().iterator(); it.hasNext();) {
        Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e = it.next();
        PrimaryIndex primaryIndex = e.getKey().getPrimaryIndex();
        if (primaryIndex.containsKey(e.getKey().getObject())) {
          txn.getReplaced().put(e.getKey(), e.getValue());
          moved.add(e.getKey());
          it.remove();
        }
      }

      for (Iterator<Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity>> it = txn.getReplaced().entrySet().iterator(); it.hasNext();) {
        Map.Entry<PrimaryIndex.CachedKey, PrimaryIndex.CachedEntity> e = it.next();
        if (!moved.contains(e.getKey())) {
          PrimaryIndex primaryIndex = e.getKey().getPrimaryIndex();
          if (!primaryIndex.containsKey(e.getKey().getObject())) {
            txn.getCreated().put(e.getKey(), e.getValue());
            it.remove();
          }
        }
      }

    }
  }

}
