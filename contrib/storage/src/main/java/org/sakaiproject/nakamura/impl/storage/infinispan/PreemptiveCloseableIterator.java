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
package org.sakaiproject.nakamura.impl.storage.infinispan;

import org.infinispan.query.QueryIterator;
import org.sakaiproject.nakamura.api.storage.CloseableIterator;
import org.sakaiproject.nakamura.api.storage.Entity;

import java.io.IOException;

/**
 * A closeable iterator that closes over an infinispan query iterator. It performs a
 * simple look-ahead approache to ensuring nulls don't creep out of the search results.
 * 
 * TODO: why do nulls creep out of the search results?
 */
public class PreemptiveCloseableIterator implements CloseableIterator<Entity> {

  private QueryIterator i;
  private Entity next;

  public PreemptiveCloseableIterator(QueryIterator i) {
    this.i = i;
    fetchNext();
  }
  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Entity next() {
    Entity next = this.next;
    fetchNext();
    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove document from query iterator.");
  }

  @Override
  public void close() throws IOException {
    i.close();
  }
  
  private void fetchNext() {
    this.next = null;
    while (i.hasNext()) {
      this.next = (Entity) i.next();
      if (this.next != null)
        break;
    }
  }
}
