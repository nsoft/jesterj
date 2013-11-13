package org.solrsystem.ingest.model.impl;

import com.google.common.collect.*;

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
 * are of type @link(java.lang.String} should be submitted as a field & value to solr. Multiple
 * values for the same field are supported and addition order is maintained. The file data
 * will be discarded by default, and if it is to be indexed by solr it should be processed
 * and the text result added as a string value by a step in a plan that processes this item.
 *
 * @see ForwardingListMultimap
 */
public class Item extends ForwardingListMultimap<String,String> {

  private ArrayListMultimap<String, String> delegate = ArrayListMultimap.create();
  private byte[] rawData;

  public Multiset<String> keys() {
    return delegate.keys();
  }

  public boolean putAll(@javax.annotation.Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.putAll(key, values);
  }

  public boolean put(@javax.annotation.Nullable java.lang.String key, @javax.annotation.Nullable java.lang.String value) {
    return delegate.put(key, value);
  }

  public boolean putAll(Multimap<? extends String, ? extends String> multimap) {
    return delegate.putAll(multimap);
  }

  public Set<String> keySet() {
    return delegate.keySet();
  }

  public boolean containsEntry(@javax.annotation.Nullable java.lang.Object key, @javax.annotation.Nullable java.lang.Object value) {
    return delegate.containsEntry(key, value);
  }

  public boolean remove(@javax.annotation.Nullable java.lang.Object key, @javax.annotation.Nullable java.lang.Object value) {
    return delegate.remove(key, value);
  }

  public boolean containsValue(@javax.annotation.Nullable java.lang.Object value) {
    return delegate.containsValue(value);
  }

  public Collection<Map.Entry<String,String>> entries() {
    return delegate.entries();
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  public void clear() {
    delegate.clear();
  }

  public Map<String,Collection<String>> asMap() {
    return delegate.asMap();
  }

  public List<String> replaceValues(@javax.annotation.Nullable java.lang.String key, Iterable<? extends String> values) {
    return delegate.replaceValues(key, values);
  }

  public Collection<String> values() {
    return delegate.values();
  }

  public void trimToSize() {
    delegate.trimToSize();
  }

  public boolean containsKey(@javax.annotation.Nullable java.lang.Object key) {
    return delegate.containsKey(key);
  }

  @Override
  protected ListMultimap<String, String> delegate() {
    return delegate;
  }

  public List<String> get(@javax.annotation.Nullable java.lang.String key) {
    return delegate.get(key);
  }

  public int size() {
    return delegate.size();
  }

  public List<String> removeAll(@javax.annotation.Nullable java.lang.Object key) {
    return delegate.removeAll(key);
  }

  public byte[] getRawData() {
    return rawData;
  }

  public void setRawData(byte[] rawData) {
    this.rawData = rawData;
  }
}
