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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * An interprocess mutex lock.
 * <p>Typical use might look like:<pre>
 * new Lock.With(directory.makeLock("my.lock")) {
 *     public Object doBody() {
 *       <i>... code to execute while locked ...</i>
 *     }
 *   }.run();
 * </pre>
 */
public abstract class Lock {

  private Set<LockListener> listeners = new LinkedHashSet<LockListener>();

  public Set<LockListener> getListeners() {
    return listeners;
  }
  
  /**
   * How long {@link #obtain(long)} waits, in milliseconds,
   * in between attempts to acquire the lock.
   */
  public static long LOCK_POLL_INTERVAL = 1000;

  /**
   * Pass this value to {@link #obtain(long)} to try
   * forever to obtain the lock.
   */
  public static final long LOCK_OBTAIN_WAIT_FOREVER = -1;

  /**
   * Avoids deadlocks when the lock holder attempts at concurrent locking an already held lock.
   */
  private AtomicInteger lockDepth = new AtomicInteger(0);

  /**
   * Attempts to obtain exclusive access and immediately return
   * upon success or failure.
   *
   * @return true iff exclusive access is obtained
   * @throws IOException
   */
  public final synchronized boolean obtain() throws IOException {
    boolean obtained;
    int depth = 0;
    if (lockDepth.get() > 0) {
      depth = lockDepth.incrementAndGet();
      obtained = true;
    } else if (obtained = doObtain()) {
      depth = lockDepth.incrementAndGet();
    }

    if (obtained) {
      for (LockListener listener : getListeners()) {
        listener.obtained(this, depth);
      }
    }
    return obtained;
  }


  protected abstract boolean doObtain() throws IOException;


  /**
   * If a lock obtain called, this failureReason may be set
   * with the "root cause" Exception as to why the lock was
   * not obtained.
   */
  protected Throwable failureReason;

  /**
   * Attempts to obtain an exclusive lock within amount of
   * time given. Polls once per {@link #LOCK_POLL_INTERVAL}
   * (currently 1000) milliseconds until lockWaitTimeout is
   * passed.
   *
   * @param lockWaitTimeout length of time to wait in
   *                        milliseconds or {@link
   *                        #LOCK_OBTAIN_WAIT_FOREVER} to retry forever
   * @return true if lock was obtained
   * @throws LockObtainFailedException if lock wait times out
   * @throws IllegalArgumentException  if lockWaitTimeout is
   *                                   out of bounds
   * @throws IOException               if obtain() throws IOException
   */
  public boolean obtain(long lockWaitTimeout) throws LockObtainFailedException, IOException {
    failureReason = null;
    boolean locked = obtain();
    if (lockWaitTimeout < 0 && lockWaitTimeout != LOCK_OBTAIN_WAIT_FOREVER)
      throw new IllegalArgumentException("lockWaitTimeout should be LOCK_OBTAIN_WAIT_FOREVER or a non-negative number (got " + lockWaitTimeout + ")");

    long maxSleepCount = lockWaitTimeout / LOCK_POLL_INTERVAL;
    long sleepCount = 0;
    while (!locked) {
      if (lockWaitTimeout != LOCK_OBTAIN_WAIT_FOREVER && sleepCount++ >= maxSleepCount) {
        String reason = "Lock obtain timed out: " + this.toString();
        if (failureReason != null) {
          reason += ": " + failureReason;
        }
        LockObtainFailedException e = new LockObtainFailedException(reason);
        if (failureReason != null) {
          e.initCause(failureReason);
        }
        throw e;
      }
      try {
        Thread.sleep(LOCK_POLL_INTERVAL);
      } catch (InterruptedException e) {
        throw new IOException(e.toString());
      }
      locked = obtain();
    }
    return locked;
  }

  /**
   * Releases exclusive access.
   */
  public final synchronized void release() throws IOException {
    int depth = 0;
    boolean released;
    if (lockDepth.get() > 1) {
      depth = lockDepth.decrementAndGet();
      released = true;
    } else if (released = doRelease()) {
      depth  = lockDepth.decrementAndGet();
    }
    if (released) {
      for (LockListener listener : getListeners()) {
        listener.released(this, depth);
      }
    }

  }

  /**
   * Releases exclusive access.
   */
  public abstract boolean doRelease() throws IOException;


  /**
   * Returns true if the resource is currently locked.  Note that one must
   * still call {@link #obtain()} before using the resource.
   */
  public abstract boolean isLocked();


  /**
   * Utility class for executing code with exclusive access.
   */
  public abstract static class With<T> {
    private Lock lock;
    private long lockWaitTimeout;    
    
    /**
     * Constructs an executor that will grab the named lock.
     */
    public With(Lock lock, long lockWaitTimeout) {
      this.lock = lock;
      this.lockWaitTimeout = lockWaitTimeout;
    }

    /**
     * Code to execute with exclusive access.
     */
    public abstract T doBody() throws IOException;

    /**
     * Calls {@link #doBody} while <i>lock</i> is obtained.  Blocks if lock
     * cannot be obtained immediately.  Retries to obtain lock once per second
     * until it is obtained, or until it has tried ten times. Lock is released when
     * {@link #doBody} exits.
     *
     * @throws LockObtainFailedException if lock could not
     *                                   be obtained
     * @throws IOException               if {@link Lock#obtain} throws IOException
     */
    public T run() throws LockObtainFailedException, IOException {
      boolean locked = false;
      try {
        locked = lock.obtain(lockWaitTimeout);
        return doBody();
      } finally {
        if (locked)
          lock.release();
      }
    }
  }

}
