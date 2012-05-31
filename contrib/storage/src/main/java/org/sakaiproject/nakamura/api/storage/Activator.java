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
package org.sakaiproject.nakamura.api.storage;

import java.io.IOException;
import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;

/**
 *
 */
public class Activator implements BundleActivator, ServiceListener {

  // copied the FQCN to keep from binding to the impl
  private final static String CONFIG_NAME = "org.sakaiproject.nakamura.storage.cassandra.CassandraStorageService-connections";
  private final ServiceTracker configTracker;

  public Activator(BundleContext context) {
    configTracker = new ServiceTracker(context, ConfigurationAdmin.class.getName(), null);
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
   */
  @Override
  public void start(BundleContext context) throws Exception {
    configTracker.open();
    if (configTracker.getService() != null) {
      this.serviceChanged(new ServiceEvent(ServiceEvent.REGISTERED, context
          .getServiceReference(ConfigurationAdmin.class.getName())));
    }
    context.addServiceListener(this);
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
   */
  @Override
  public void stop(BundleContext context) throws Exception {
    context.removeServiceListener(this);
    configTracker.close();
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.osgi.framework.ServiceListener#serviceChanged(org.osgi.framework.ServiceEvent)
   */
  public void serviceChanged(ServiceEvent event) {
    if (event.getType() == ServiceEvent.REGISTERED && configTracker.getService() != null) {
      ConfigurationAdmin configAdmin = (ConfigurationAdmin) configTracker.getService();
      try {
        Configuration[] configs = configAdmin.listConfigurations("service.pid=" + CONFIG_NAME);
        if (configs == null) {
          Configuration config = configAdmin.createFactoryConfiguration(CONFIG_NAME);
          Hashtable<String, Object> props = new Hashtable<String, Object>();
          props.put("clusterName", "SakaiOAE");
          props.put("keyspaceName", "SakaiOAE");
          config.update(null);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InvalidSyntaxException e) {
        e.printStackTrace();
      }
    }
  }
}
