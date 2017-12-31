package org.atri.platodb.entity.isolation;

import org.atri.platodb.entity.Transaction;

import java.io.IOException;

/**
 * Interface to be implemented for Isolation mechanisms
 * 
 * @author atri
 * @since 2017-mar-14 15:39:32
 */
public interface IsolationStrategy {

  public abstract void checkVersion(Transaction txn) throws IOException;
  public abstract int getCountModifier();

}
