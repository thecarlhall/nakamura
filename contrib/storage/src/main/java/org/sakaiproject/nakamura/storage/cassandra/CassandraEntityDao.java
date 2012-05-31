/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.storage.cassandra;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.lucene.search.Query;
import org.sakaiproject.nakamura.api.storage.Entity;
import org.sakaiproject.nakamura.api.storage.EntityDao;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.ColumnFamilyQuery;

/**
 *
 */
@Component(componentAbstract =  true)
public abstract class CassandraEntityDao<T extends Entity> implements EntityDao<T> {

  protected Keyspace keyspace;
  protected ColumnFamily<K, C> columnFamily;

//  public CassandraEntityDao() {
//  }
//
  public CassandraEntityDao(Keyspace keyspace, ColumnFamily<K, C> columnFamily) {
    this.keyspace = keyspace;
    this.columnFamily = columnFamily;
  }

  public abstract CassandraEntityDao<T> newInstance(Class<T> entity, Keyspace keyspace, ColumnFamily<String, String> columnFamily);

  protected abstract <K, C> ColumnFamily<K, C> getColumnFamily();
  protected abstract <C> void store(T entity, ColumnListMutation<C> mutations);

  @Override
  public <K, C> T get(K key) {
    try {
      ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(getColumnFamily());
      OperationResult<ColumnList<C>> result = query.getKey(key).execute();
      ColumnList<C> columns = result.getResult();
//    columns.get
//    BeanUtils.populate(columns, null);
    } catch (ConnectionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public T update(T entity) {
    Map<String, String> props;
    try {
      props = BeanUtils.describe(entity);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    MutationBatch m = keyspace.prepareMutationBatch();
    ColumnListMutation<String> mutations = m.withRow(getColumnFamily(), entity.getKey());

    for (Entry<String, String> prop : props.entrySet()) {
      /*
       * TODO look into using BeanUtils to get fields from entity
       * TODO Use JPA rules, annotations for Entity, Field.
       * TODO Cache processed info about entity if not found.
       * TODO Provide way to clear the cache or eviction policy.
       */
      // TODO use instanceof to set value
////    updater.setByteArray(null, null);
      mutations.putColumn(prop.getKey(), prop.getValue(), null);
    }
    try {
      OperationResult<Void> result = m.execute();
      Void v = result.getResult();
    } catch (ConnectionException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return entity;
  }

  @Override
  public List<T> findAll(Query query) {
    return null;
  }
}
