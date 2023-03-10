package org.jesterj.ingest.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ColDefs implements ColumnDefinitions {

  public static final DefaultCodecRegistry CODEC_REGISTRY = new DefaultCodecRegistry("jj_");
  private final ColumnDefinitions delegate;

  public ColDefs(ColumnDefinitions delegate) {
    this.delegate = delegate;
  }

  public static ColDefs notStupid(ColumnDefinitions delegate) {
    return new ColDefs(delegate);
  }

  public String[] getNames() {
    return listNames().toArray(new String[]{});
  }

  public List<String> listNames() {
    List<ColumnDefinition> defs = StreamSupport.stream(delegate.spliterator(), false).collect(Collectors.toList());
    return defs.stream().map(ColumnDefinition::getName).map(CqlIdentifier::asInternal).collect(Collectors.toList());
  }

  public List<String> listValues(Row row) {
    List<String> values = new ArrayList<>();
    List<String> colNames = listNames();
    for (String colName : colNames) {
      DataType type = row.getColumnDefinitions().get(colName).getType();
      TypeCodec<?> driverCodec = CODEC_REGISTRY.codecFor(type);
      Object obj = row.get(colName, driverCodec);
      values.add(String.valueOf(obj));
    }
    return values;
  }

  public String[] getValues(Row row) {
    return listValues(row).toArray(new String[]{});
  }

  public int size() {
    return delegate.size();
  }

  @NonNull
  public ColumnDefinition get(int i) {
    return delegate.get(i);
  }

  @NonNull
  public ColumnDefinition get(@NotNull String name) {
    return delegate.get(name);
  }

  @NonNull
  public ColumnDefinition get(@NotNull CqlIdentifier name) {
    return delegate.get(name);
  }

  public boolean contains(@NotNull String name) {
    return delegate.contains(name);
  }

  public boolean contains(@NotNull CqlIdentifier id) {
    return delegate.contains(id);
  }

  @NonNull
  public List<Integer> allIndicesOf(@NotNull String name) {
    return delegate.allIndicesOf(name);
  }

  public int firstIndexOf(@NotNull String name) {
    return delegate.firstIndexOf(name);
  }

  @NonNull
  public List<Integer> allIndicesOf(@NotNull CqlIdentifier id) {
    return delegate.allIndicesOf(id);
  }

  public int firstIndexOf(@NotNull CqlIdentifier id) {
    return delegate.firstIndexOf(id);
  }

  public Iterator<ColumnDefinition> iterator() {
    return delegate.iterator();
  }

  public void forEach(Consumer<? super ColumnDefinition> action) {
    delegate.forEach(action);
  }

  public Spliterator<ColumnDefinition> spliterator() {
    return delegate.spliterator();
  }

  public boolean isDetached() {
    return delegate.isDetached();
  }

  public void attach(@NotNull AttachmentPoint attachmentPoint) {
    delegate.attach(attachmentPoint);
  }


}
