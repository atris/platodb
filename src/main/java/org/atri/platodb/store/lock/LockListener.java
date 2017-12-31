package org.atri.platodb.store.lock;

/**
 *
 *
 * @author atri
 * @since 2017-mar-21 03:04:07
 */
public interface LockListener {

  public abstract void obtained(Lock lock, int depth);
  public abstract void released(Lock lock, int depth);

// todo this could be useful?
//  public abstract void obtained(Lock.With lock, int depth);
//  public abstract void released(Lock.With lock, int depth);

}
