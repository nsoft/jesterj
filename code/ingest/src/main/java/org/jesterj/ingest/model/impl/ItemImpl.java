package org.jesterj.ingest.model.impl;

import com.google.common.collect.*;
import org.jesterj.ingest.model.Item;
import org.jesterj.ingest.model.Scanner;
import org.jesterj.ingest.model.Status;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/10/13
 */

/**
 * A container for the file data and associated metadata. MetaData for which the key and the value
 * are of type @link(java.lang.String} should be submitted as a field & value to the index. Multiple
 * values for the same field are supported and addition order is maintained. The file data
 * will be discarded by default, and if it is to be indexed, it should be processed
 * and the text result added as a string value by a step in a plan that processes this item.
 *
 * @see ForwardingListMultimap
 */
public class ItemImpl implements Item {

  private ArrayListMultimap<String, String> delegate = ArrayListMultimap.create();
  private byte[] rawData;
  private volatile int queueEntryNumber;
  private Scanner source;
  private Status status = Status.PROCESSING;
  private String statusMessage;

  public ItemImpl(Scanner src)  {
    this.source = src;
  }


  @Override
  public Multiset<String> keys() {
    return delegate.keys();
  }

  @Override
  public boolean putAll(@Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.putAll(key, values);
  }

  @Override
  public boolean put(@Nullable java.lang.String key, @Nullable java.lang.String value) {
    return delegate.put(key, value);
  }

  @Override
  public boolean putAll(Multimap<? extends String, ? extends String> multimap) {
    return delegate.putAll(multimap);
  }

  @Override
  public Set<String> keySet() {
    return delegate.keySet();
  }

  @Override
  public boolean containsEntry(@Nullable java.lang.Object key, @Nullable java.lang.Object value) {
    return delegate.containsEntry(key, value);
  }

  @Override
  public boolean remove(@Nullable java.lang.Object key, @Nullable java.lang.Object value) {
    return delegate.remove(key, value);
  }

  @Override
  public boolean containsValue(@Nullable java.lang.Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public Collection<Map.Entry<String,String>> entries() {
    return delegate.entries();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Map<String,Collection<String>> asMap() {
    return delegate.asMap();
  }

  @Override
  public List<String> replaceValues(@Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.replaceValues(key, values);
  }

  @Override
  public Collection<String> values() {
    return delegate.values();
  }

  @Override
  public boolean containsKey(@Nullable java.lang.Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public List<String> get(@Nullable java.lang.String key) {
    return delegate.get(key);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public List<String> removeAll(@Nullable java.lang.Object key) {
    return delegate.removeAll(key);
  }

  @Override
  public byte[] getRawData() {
    return rawData;
  }

  @Override
  public void setRawData(byte[] rawData) {
    this.rawData = rawData;
  }


  @Override
  public Scanner getSource() {
    return this.source;
  }

  @Override
  public Status getStatus() {
    return this.status;
  }

  @Override
  public void setStatus(Status status) {
    this.status = status;
  }

  @Override
  public String getStatusMessage() {
    return statusMessage;
  }

  @Override
  public void setStatusMessage(String message) {
    this.statusMessage = message;
  }
}
