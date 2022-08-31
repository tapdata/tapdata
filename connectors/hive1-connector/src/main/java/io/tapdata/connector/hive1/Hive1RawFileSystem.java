package io.tapdata.connector.hive1;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Hive1RawFileSystem extends RawLocalFileSystem {
  private static final URI NAME;

  static {
    try {
      NAME = new URI("raw:///");
    } catch (URISyntaxException se) {
      throw new IllegalArgumentException("bad uri", se);
    }
  }

  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    File file = pathToFile(path);
    if (!file.exists()) {
      throw new FileNotFoundException("Can't find " + path);
    }
    // get close enough
    short mod = 0;
    if (file.canRead()) {
      mod |= 0444;
    }
    if (file.canWrite()) {
      mod |= 0200;
    }
    if (file.canExecute()) {
      mod |= 0111;
    }
    return new FileStatus(file.length(), file.isDirectory(), 1, 1024, file.lastModified(), file.lastModified(),
      FsPermission.createImmutable(mod), "owen", "users", path);
  }
}
