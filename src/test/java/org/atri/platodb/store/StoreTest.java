package org.atri.platodb.store;

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


import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.atri.platodb.entity.EntityStore;

/**
 * @author atri
 * @since 2017-mar-17 00:59:03
 */
public abstract class StoreTest extends TestCase {

  private File path;

  protected StoreTest() {
    path = new File("target/test-data/stores");
    if (!path.exists() && !path.mkdirs()) {
      throw new RuntimeException("Could not create path " + path);
    }

  }
  
  protected File getDirectory(String name) {
    File path = new File(this.path, name);
    if (path.exists()) {
      FileUtils.deleteQuietly(path);
    }
    if (!path.mkdirs()) {
      throw new RuntimeException("Could not create path " + path);
    }
    return path;
  }

  protected byte[][] keys;
  protected long[] hashes;
  protected byte[][] values;


  protected void randomizeKeyValues(int items) {

    long seed = System.currentTimeMillis();
    System.err.println("seed = " + seed);
    Random random = new Random(seed);


    keys = new byte[items][];
    hashes = new long[items];
    values = new byte[items][];

    for (int i = 0; i < items; i++) {
      keys[i] = new byte[random.nextInt(10)+1];
      random.nextBytes(keys[i]);
      hashes[i] = random.nextLong();
      values[i] = new byte[random.nextInt(1000)];
      random.nextBytes(values[i]);
    }
  }

}
