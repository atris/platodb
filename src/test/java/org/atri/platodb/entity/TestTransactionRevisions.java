package org.atri.platodb.entity;

import org.junit.Test;
import org.atri.platodb.store.Accessor;
import org.atri.platodb.store.data.Metadata;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author atri
 * @since 2017-mar-27 18:34:19
 */
public class TestTransactionRevisions extends EntityStoreTest {
  
  @Test
  public void testTransactionRevisionsWithNonDurablePostings() throws IOException {

    EntityStore store = entityStoreFactory("entityStore/testTransactionRevisionsWithNonDurablePostings");
    store.getConfiguration().setUsingDurablePostingLinks(false);
    PrimaryIndex<Long, EntityClass> index = store.getPrimaryIndex(Long.class, EntityClass.class);

    assertEquals(0l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(0l, "A"));
    store.getTxn().commit();

    assertEquals(1l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(1l, "B"));
    index.put(new EntityClass(2l, "C"));
    store.getTxn().commit();

    assertEquals(2l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(3l, "D"));
    index.put(new EntityClass(4l, "E"));
    index.remove(1l);
    store.getTxn().commit();

    assertEquals(3l, store.getStoreRevision());


    store.getTxn().begin();
    index.remove(2l);
    store.getTxn().commit();

    assertEquals(4l, store.getStoreRevision());


    store.getTxn().begin();
    index.remove(3l);
    index.put(new EntityClass(4l, "F"));
    store.getTxn().commit();

    assertEquals(5l, store.getStoreRevision());

    store.getTxn().begin();

    store.getTxn().setDefaultReadRevision(0l);
    assertFalse(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertNull(index.get(0l));
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertNull(index.get(4l));



    store.getTxn().setDefaultReadRevision(1l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertNull(index.get(4l));


    store.getTxn().setDefaultReadRevision(2l);
    assertTrue(index.containsKey(0l));
    assertTrue(index.containsKey(1l));
    assertTrue(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertEquals("B", index.get(1l).getValue());
    assertEquals("C", index.get(2l).getValue());
    assertNull(index.get(3l));
    assertNull(index.get(4l));


    store.getTxn().setDefaultReadRevision(3l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertTrue(index.containsKey(2l));
    assertTrue(index.containsKey(3l));
    assertFalse(index.containsKey(4l)); // non durable, deleted in rev 5 for F, so it looks like never created in this revision
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertEquals("C", index.get(2l).getValue());
    assertEquals("D", index.get(3l).getValue());
    assertNull(index.get(4l)); // non durable, deleted i rev 5 for F, so it seems to be never created in this rev

    store.getTxn().setDefaultReadRevision(4l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertTrue(index.containsKey(3l));
    assertFalse(index.containsKey(4l)); // non durable, deleted in rev 5 for F, so it looks like never created in this revision
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertEquals("D", index.get(3l).getValue());
    assertNull(index.get(4l)); // non durable, deleted i rev 5 for F, so it seems to be never created in this rev

    store.getTxn().setDefaultReadRevision(5l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertEquals("F", index.get(4l).getValue());

    store.getTxn().setDefaultReadRevision(Long.MAX_VALUE);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertEquals("F", index.get(4l).getValue());

    store.getTxn().abort();
  }


  @Test
  public void testTransactionRevisionsWithDurablePostings() throws IOException {

    EntityStore store = entityStoreFactory("entityStore/testTransactionRevisionsWithDurablePostings");
    store.getConfiguration().setUsingDurablePostingLinks(true);

    PrimaryIndex<Long, EntityClass> index = store.getPrimaryIndex(Long.class, EntityClass.class);

    assertEquals(0l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(0l, "A"));
    store.getTxn().commit();

    assertEquals(1l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(1l, "B"));
    index.put(new EntityClass(2l, "C"));
    store.getTxn().commit();

    assertEquals(2l, store.getStoreRevision());


    store.getTxn().begin();
    index.put(new EntityClass(3l, "D"));
    index.put(new EntityClass(4l, "E"));
    index.remove(1l);
    store.getTxn().commit();

    assertEquals(3l, store.getStoreRevision());


    store.getTxn().begin();
    index.remove(2l);
    store.getTxn().commit();

    assertEquals(4l, store.getStoreRevision());


    store.getTxn().begin();
    index.remove(3l);
    index.put(new EntityClass(4l, "F"));
    store.getTxn().commit();

    assertEquals(5l, store.getStoreRevision());

    store.getTxn().begin();

    store.getTxn().setDefaultReadRevision(0l);
    assertFalse(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertNull(index.get(0l));
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertNull(index.get(4l));



    store.getTxn().setDefaultReadRevision(1l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertNull(index.get(4l));


    store.getTxn().setDefaultReadRevision(2l);
    assertTrue(index.containsKey(0l));
    assertTrue(index.containsKey(1l));
    assertTrue(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertFalse(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertEquals("B", index.get(1l).getValue());
    assertEquals("C", index.get(2l).getValue());
    assertNull(index.get(3l));
    assertNull(index.get(4l));


    store.getTxn().setDefaultReadRevision(3l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertTrue(index.containsKey(2l));
    assertTrue(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertEquals("C", index.get(2l).getValue());
    assertEquals("D", index.get(3l).getValue());
    assertEquals("E", index.get(4l).getValue());

    store.getTxn().setDefaultReadRevision(4l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertTrue(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertEquals("D", index.get(3l).getValue());
    assertEquals("E", index.get(4l).getValue());

    store.getTxn().setDefaultReadRevision(5l);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertEquals("F", index.get(4l).getValue());

    store.getTxn().setDefaultReadRevision(Long.MAX_VALUE);
    assertTrue(index.containsKey(0l));
    assertFalse(index.containsKey(1l));
    assertFalse(index.containsKey(2l));
    assertFalse(index.containsKey(3l));
    assertTrue(index.containsKey(4l));
    assertEquals("A", index.get(0l).getValue());
    assertNull(index.get(1l));
    assertNull(index.get(2l));
    assertNull(index.get(3l));
    assertEquals("F", index.get(4l).getValue());

    store.getTxn().abort();
  }


  @Entity
  public static class EntityClass implements Serializable {
    @PrimaryKey
    private Long id;

    private String value;

    public EntityClass(Long id, String value) {
      this.id = id;
      this.value = value;
    }

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
}
