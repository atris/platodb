package org.atri.platodb.entity;

import org.atri.platodb.store.*;
import org.atri.platodb.store.sequence.UnitTestSequenceManager;

import java.io.IOException;

/**
 * @author atri
 * @since 2017-mar-23 00:46:21
 */
public abstract class EntityStoreTest extends StoreTest {

  protected EntityStore entityStoreFactory(String name) throws IOException {
    EntityStore entityStore = new EntityStore(new Configuration(getDirectory(name)));
    entityStore.getConfiguration().setHashCodesPartitionByteSize(Configuration.megaByte);
    entityStore.getConfiguration().setKeysPartitionByteSize(Configuration.megaByte);
    entityStore.getConfiguration().setValuesPartitionByteSize(Configuration.megaByte);
    entityStore.open();
    return entityStore;
  }


  protected EntityStore reopen(EntityStore entityStore) throws IOException {
    entityStore.close();
    entityStore = new EntityStore(entityStore.getConfiguration());
    entityStore.open();
    return entityStore;
  }

}
