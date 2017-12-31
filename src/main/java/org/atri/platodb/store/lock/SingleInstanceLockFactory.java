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
import java.util.HashSet;

/**
 * Implements {@link LockFactory} for a single in-process instance,
 * meaning all locking will take place through this one instance.
 * Only use this {@link LockFactory} when you are certain all
 * IndexReaders and IndexWriters for a given index are running
 * against a single shared in-process Directory instance.  This is
 * currently the default locking for RAMDirectory.
 *
 * @see LockFactory
 */

public class SingleInstanceLockFactory extends LockFactory {

  private HashSet locks = new HashSet();

  public Lock makeLock(String lockName) {
    // We do not use the LockPrefix at all, because the private
    // HashSet instance effectively scopes the locking to this
    // single Directory instance.
    return new SingleInstanceLock(locks, lockName);
  }

  public void clearLock(String lockName) throws IOException {
    synchronized(locks) {
      if (locks.contains(lockName)) {
        locks.remove(lockName);
      }
    }
  }
};

class SingleInstanceLock extends Lock {

  String lockName;
  private HashSet locks;

  public SingleInstanceLock(HashSet locks, String lockName) {
    this.locks = locks;
    this.lockName = lockName;
  }

  public boolean doObtain() throws IOException {
    synchronized(locks) {
      return locks.add(lockName);
    }
  }

  public boolean doRelease() {
    synchronized(locks) {
      return locks.remove(lockName);
    }
  }

  public boolean isLocked() {
    synchronized(locks) {
      return locks.contains(lockName);
    }
  }

  public String toString() {
    return super.toString() + ": " + lockName;
  }
}
