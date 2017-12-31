package org.atri.platodb.entity.serialization;

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


import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Handles gzipped or non gzipped serializable object
 *
 * @see java.io.Serializable
 * @see org.atri.platodb.entity.serialization.SerializableMarshaller
 * @author atri
 * @since 2017-mar-17 06:34:08
 */
public class SerializableUnmarshaller extends Unmarshaller {

  public SerializableUnmarshaller() {
  }

  public Serializable unmarshall(byte[] bytes, int startOffset, int length) throws IOException {
    InputStream in = new ByteArrayInputStream(bytes, startOffset, length);
    if (length > 2 && bytes[startOffset] == 31 && bytes[startOffset+1] == -117) {
      // gzip magic numbers todo create our own two byte header insead! 
      in = new GZIPInputStream(in);
    }
    ObjectInputStream oos = new ObjectInputStream(in);
    Serializable object;
    try {
      object = (Serializable) oos.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    oos.close();
    return object;
  }
}
