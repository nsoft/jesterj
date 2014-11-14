package org.jesterj.ingest.model.impl;

import com.google.common.collect.*;
import net.jini.core.entry.Entry;
import org.jesterj.ingest.model.Item;
import org.jesterj.ingest.model.Plan;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.Step;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
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

  // document id feild.
  private final String ID;

  private ArrayListMultimap<String, String> delegate = ArrayListMultimap.create();
  private byte[] rawData;
  private Status status = Status.PROCESSING;
  private String statusMessage = "";

  private Plan plan;

  public ItemImpl(byte[] rawData, String id, Plan plan) {
    this.rawData = rawData;
    ID = plan.getDocIdField();
    this.delegate.put(ID, id);
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
  public boolean put(@Nonnull java.lang.String key, @Nonnull java.lang.String value) {
    if (plan.getDocIdField().equals(key)) {
      ArrayList<String> values = new ArrayList<>();
      values.add(value);
      List<String> prev = replaceValues(this.ID, values);
      return prev == null || prev.size() != 1 || !prev.get(0).equals(value);
    } else {
      return delegate.put(key, value);
    }
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
  public Collection<Map.Entry<String, String>> entries() {
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
  public Map<String, Collection<String>> asMap() {
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

  @Override
  public void setPlan(Plan plan) {
    this.plan = plan;
  }

  @Override
  public Entry toEntry(Step next) {
    return new ItemEntry(this, next);
  }

  @Override
  public ArrayListMultimap<String, String> getDelegate() {
    return delegate;
  }

  /**
   * A serializable form of an item that
   */
  public static class ItemEntry implements Entry {

    public ArrayListMultimap<String, String> contents;
    public String scannerName;
    public Status status;
    public String statusMessage;
    public RawData data;
    public String nextStepName;

    ItemEntry(Item item, Step destination) {
      this.contents = item.getDelegate();
      this.status = item.getStatus();
      this.statusMessage = item.getStatusMessage();
      this.data = new RawData();
      this.data.data = item.getRawData();
      this.nextStepName = destination.getName();
    }
  }

  public static class RawData {
    public byte[] data;
  }
}
