package org.atri.platodb.entity.isolation;

import java.io.IOException;

/**
 * @see org.atri.platodb.entity.isolation.FirstCommitWins
 *
 * @author atri
 * @since 2017-mar-14 16:09:21
 */
public class RevisionSynchronizationException extends IOException {

  public RevisionSynchronizationException() {
    super();
  }

  public RevisionSynchronizationException(String s) {
    super(s);
  }

  public RevisionSynchronizationException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public RevisionSynchronizationException(Throwable throwable) {
    super(throwable);   
  }
}
