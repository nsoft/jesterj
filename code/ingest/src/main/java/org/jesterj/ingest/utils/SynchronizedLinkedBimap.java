package org.jesterj.ingest.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.*;

/**
 * This is a trivial modification of org.apache.cassandra.utils.ConcurrentBiMap such that it
 * preserves order, and is synchronized rather than concurrent. All methods are synchronized. The reverse view is immutable.
 * <p>
 * Iteration is still vulnerable to concurrent modification exceptions, but JesterJ only iterates this object
 * in cases where it's been sequestered (and replaced) so we should never have changes to the map
 * when another thread is iterating it anyway. There isn't a lot of opportunity for concurrent multi-threaded
 * access in the code, but it's super critical that happens-before memory model relationships are always consistent
 * for this collection, and future features may introduce more threads.
 *
 * @param <K> the type for keys
 * @param <V> the type for values
 */
public class SynchronizedLinkedBimap<K, V> implements Map<K, V>
{
  protected final Map<K, V> forwardMap;
  protected final Map<V, K> reverseMap;

  public SynchronizedLinkedBimap()
  {
    this(Collections.synchronizedMap(new LinkedHashMap<>()), Collections.synchronizedMap(new LinkedHashMap<>()));
  }

  /**
   * Instantiate a new instance with specific maps. Supplied maps MUST be thread safe
   *
   * @param forwardMap a thread safe implementation of Map with keys that are values in reverseMap
   *                   and values that are keys in reverseMap
   * @param reverseMap a thread safe implementation of Map with keys that are values in forwardMap
   *                   and values that are keys in forwardMap
   */
  protected SynchronizedLinkedBimap(Map<K, V> forwardMap, Map<V, K> reverseMap)
  {
    this.forwardMap = forwardMap;
    this.reverseMap = reverseMap;
  }

  public Map<V, K> inverse()
  {
    return Collections.unmodifiableMap(reverseMap);
  }

  public synchronized void clear()
  {
    forwardMap.clear();
    reverseMap.clear();
  }

  public synchronized boolean containsKey(Object key)
  {
    return forwardMap.containsKey(key);
  }

  public synchronized boolean containsValue(Object value)
  {
    //noinspection SuspiciousMethodCalls
    return reverseMap.containsKey(value);
  }

  public synchronized Set<Entry<K, V>> entrySet()
  {
    return forwardMap.entrySet();
  }

  public synchronized V get(Object key)
  {
    return forwardMap.get(key);
  }

  public synchronized boolean isEmpty()
  {
    return forwardMap.isEmpty();
  }

  public synchronized Set<K> keySet()
  {
    return forwardMap.keySet();
  }

  public synchronized V put(K key, V value)
  {
    K oldKey = reverseMap.get(value);
    if (oldKey != null && !key.equals(oldKey))
      throw new IllegalArgumentException(value + " is already bound in reverseMap to " + oldKey);
    V oldVal = forwardMap.put(key, value);
    if (oldVal != null && !Objects.equals(reverseMap.remove(oldVal), key))
      throw new IllegalStateException(); // for the prior mapping to be correct, we MUST get back the key from the reverseMap
    reverseMap.put(value, key);
    return oldVal;
  }

  public synchronized void putAll(Map<? extends K, ? extends V> m)
  {
    for (Entry<? extends K, ? extends V> entry : m.entrySet())
      put(entry.getKey(), entry.getValue());
  }

  public synchronized V remove(Object key)
  {
    V oldVal = forwardMap.remove(key);
    if (oldVal == null)
      return null;
    Object oldKey = reverseMap.remove(oldVal);
    if (oldKey == null || !oldKey.equals(key))
      throw new IllegalStateException(); // for the prior mapping to be correct, we MUST get back the key from the reverseMap
    return oldVal;
  }

  public synchronized int size()
  {
    return forwardMap.size();
  }

  public synchronized Collection<V> values()
  {
    return reverseMap.keySet();
  }
}
