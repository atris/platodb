package org.atri.platodb.store;

import java.io.IOException;

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


/**
 * Cursor implementations must be thread safe!!!
 * The easiet way is to synchronize the next() method
 *
 * @author atri
 * @since 2017-mar-16 15:37:56
 */
public interface Cursor<T> {

  public abstract T next(Accessor accessor, T object, long revision) throws IOException;

}
