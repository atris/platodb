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


import org.junit.Test;

import java.io.Serializable;

/**
 * @author atri
 * @since 2017-mar-17 07:13:35
 */
public class TestEntityStore extends EntityStoreTest {

  @Test
  public void testEntityCursor() throws Exception {

    EntityStore store = entityStoreFactory("entityStore/testEntityCursor");

    PrimaryIndex<Long, Message> messages = store.getPrimaryIndex(Long.class, Message.class);

    for (long l = 0; l < 1000; l++) {
      messages.put(new Message(l, String.valueOf(l)));
    }

    EntityCursor<Long, Message> cursor = messages.cursor();

    for (long l = 0; l < 1000; l++) {
      assertTrue(cursor.next());
      assertEquals(l, (long) cursor.key());
      assertEquals(String.valueOf(l), cursor.value().getSubject());
    }

    assertFalse(cursor.next());

    cursor.close();

    store.close();

  }


  @Test
  public void testPrimaryIndex() throws Exception {

    EntityStore store = entityStoreFactory("entityStore/testPrimaryIndex");

    assertEquals(store.getPrimaryIndex(Long.class, WithSharedSequenceNameA.class), store.getPrimaryIndex(Long.class, WithSharedSequenceNameA.class));

    try {
      store.getPrimaryIndex(String.class, WithSharedSequenceNameA.class);
      fail("Should not allow new class type");
    } catch (IllegalArgumentException e) {
      // all good
    }

    store.close();

  }

  @Test
  public void testAutoIncrement() throws Exception {

    EntityStore store = entityStoreFactory("entityStore/testAutoIncrement");

    PrimaryIndex<Long, WithSharedSequenceNameA> withSharedSequenceNamesA = store.getPrimaryIndex(Long.class, WithSharedSequenceNameA.class);
    PrimaryIndex<Long, WithSharedSequenceNameB> withSharedSequenceNamesB = store.getPrimaryIndex(Long.class, WithSharedSequenceNameB.class);

    store.getTxn().begin();

    WithSharedSequenceNameA a;
    WithSharedSequenceNameB b;
    withSharedSequenceNamesA.put(a = new WithSharedSequenceNameA());
    assertEquals(new Long(1l), a.getPK());
    withSharedSequenceNamesB.put(b = new WithSharedSequenceNameB());
    assertEquals(new Long(2l), b.getPK());

    store.getTxn().abort();

    store = reopen(store);

    withSharedSequenceNamesA = store.getPrimaryIndex(Long.class, WithSharedSequenceNameA.class);
    withSharedSequenceNamesB = store.getPrimaryIndex(Long.class, WithSharedSequenceNameB.class);
    store.getTxn().begin();
    withSharedSequenceNamesA.put(a = new WithSharedSequenceNameA());
    assertEquals(new Long(3l), a.getPK());
    store.getTxn().abort();
    store.close();
  }

  @Test
  public void testSimple() throws Exception {

    EntityStore store = entityStoreFactory("entityStore/testSimple");

    PrimaryIndex<Long, User> users = store.getPrimaryIndex(Long.class, User.class);
    PrimaryIndex<Long, Message> messages = store.getPrimaryIndex(Long.class, Message.class);

    store.getTxn().begin();

    User user = new User();
    user.setPK(0l);
    user.setEmailAdress("foo@bar.org");
    user.setPassword("secret");

    assertNull(users.put(user));

    user.setPassword("new secret");

    User oldUser = users.put(user);

    assertEquals(new Long(0l), oldUser.getPK());
    assertEquals("foo@bar.org", oldUser.getEmailAdress());
    assertEquals("secret", oldUser.getPassword());

    store.getTxn().commit();

    store.getTxn().begin();
    assertEquals("foo@bar.org", users.get(0l).getEmailAdress());
    assertEquals("new secret", users.get(0l).getPassword());
    store.getTxn().abort();

    store.getTxn().begin();

    Message message = new Message();
    message.setPK(0l);
    message.setFromUserFK(0l);
    message.setToUserFK(1l);
    message.setSubject("A second index containg an entity with the same PK as the first index.");
    message.setText("After commiting this there should be no problems still reading from both indices.");
    assertNull(messages.put(message));

    assertEquals("new secret", users.get(0l).getPassword());
    assertEquals(new Long(1l), messages.get(0l).getToUserFK());

    store.getTxn().commit();

    store.getTxn().begin();
    assertEquals("new secret", users.get(0l).getPassword());
    assertEquals(new Long(1l), messages.get(0l).getToUserFK());
    store.getTxn().abort();

    store.close();
  }

  @Entity
  public static class MissingSequenceName implements Serializable {

    private static final long serialVersionUID = 1l;

    @PrimaryKey
    private Long PK;

    public Long getPK() {
      return PK;
    }

    public void setPK(Long PK) {
      this.PK = PK;
    }
  }

  @Entity
  public static class WithSharedSequenceNameB implements Serializable {

    private static final long serialVersionUID = 1l;

    @PrimaryKey
    @Sequence(name = "my sequence")
    private Long PK;

    public Long getPK() {
      return PK;
    }

    public void setPK(Long PK) {
      this.PK = PK;
    }
  }

  @Entity
  public static class WithSharedSequenceNameA implements Serializable {

    private static final long serialVersionUID = 1l;

    @PrimaryKey
    @Sequence(name = "my sequence")
    private Long PK;

    public Long getPK() {
      return PK;
    }

    public void setPK(Long PK) {
      this.PK = PK;
    }
  }


  @Entity
  public static class User implements Serializable {

    private static final long serialVersionUID = 1l;

    @PrimaryKey
    private Long PK;

    private String emailAdress;
    private String password;

    public Long getPK() {
      return PK;
    }

    public void setPK(Long PK) {
      this.PK = PK;
    }

    public String getEmailAdress() {
      return emailAdress;
    }

    public void setEmailAdress(String emailAdress) {
      this.emailAdress = emailAdress;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }

  @Entity
  public static class Message implements Serializable {

    private static final long serialVersionUID = 1l;

    @PrimaryKey
    private Long PK;

    private Long fromUserFK;
    private Long toUserFK;
    private String subject;
    private String text;

    public Message() {
    }

    public Message(Long PK, String subject) {
      this.PK = PK;
      this.subject = subject;
    }

    public Long getPK() {
      return PK;
    }

    public void setPK(Long PK) {
      this.PK = PK;
    }

    public Long getFromUserFK() {
      return fromUserFK;
    }

    public void setFromUserFK(Long fromUserFK) {
      this.fromUserFK = fromUserFK;
    }

    public Long getToUserFK() {
      return toUserFK;
    }

    public void setToUserFK(Long toUserFK) {
      this.toUserFK = toUserFK;
    }

    public String getSubject() {
      return subject;
    }

    public void setSubject(String subject) {
      this.subject = subject;
    }

    public String getText() {
      return text;
    }

    public void setText(String text) {
      this.text = text;
    }
  }


}
