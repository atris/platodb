package org.atri.platodb.store.lock;

/**
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

import java.io.IOException;

/**
 * Use this {@link LockFactory} to disable locking entirely.
 * Only one instance of this lock is created.  You should call {@link
 * #getNoLockFactory()} to get the instance.
 *
 * @see LockFactory
 */

public class NoLockFactory extends LockFactory {

  // Single instance returned whenever makeLock is called.
  private static NoLock singletonLock = new NoLock();
  private static NoLockFactory singleton = new NoLockFactory();

  public static NoLockFactory getNoLockFactory() {
    return singleton;
  }

  public Lock makeLock(String lockName) {
    return singletonLock;
  }

  public void clearLock(String lockName) {};
};

class NoLock extends Lock {
  public boolean doObtain() throws IOException {
    return true;
  }

  public boolean doRelease() {
    return true;
  }

  public boolean isLocked() {
    return false;
  }

  public String toString() {
    return "NoLock";
  }
}
