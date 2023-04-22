package org.jesterj.ingest.scanners;

import org.jesterj.ingest.model.impl.DocumentImpl;

import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * Interface to provide some standardized default methods
 */
public interface FileScanner {

  /**
   * A default, reusable and overridable means of adding file attributes to a document
   *
   * @param attributes      The attributes of the scanned file as returned by
   *                        {@link java.nio.file.Files#getFileAttributeView(Path, Class, LinkOption...)}
   * @param doc             The document representing the scanned file to which attributes should be added.
   * @param includeAccessed if true the file accessed time will also be used. This is often not desirable.
   */
  default void addAttrs(BasicFileAttributes attributes, DocumentImpl doc, boolean includeAccessed) {
    if (attributes != null) {
      FileTime modifiedTime = attributes.lastModifiedTime();
      FileTime accessTime = attributes.lastAccessTime();
      FileTime creationTime = attributes.creationTime();
      if (modifiedTime != null) {
        doc.put("modified", String.valueOf(modifiedTime.toMillis()));
      }
      if (includeAccessed && accessTime != null) {
        doc.put("accessed", String.valueOf(accessTime.toMillis()));
      }
      if (creationTime != null) {
        doc.put("created", String.valueOf(creationTime.toMillis()));
      }
      doc.put("file_size", String.valueOf(attributes.size()));
    }
  }
}
