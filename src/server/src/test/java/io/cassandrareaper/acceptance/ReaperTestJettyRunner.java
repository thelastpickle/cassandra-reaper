/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.acceptance;

import io.cassandrareaper.SimpleReaperClient;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import com.google.common.io.Resources;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 * <p/>
 * Note, nothing in this class can import or reference any class that is in the shaded Reaper jar file.
 * All classes found in the Reaper jar file must be loaded via reflection and the ParentLastURLClassLoader.
 */
public final class ReaperTestJettyRunner {

  static final String REAPER_APPLICATION_FQN = "io.cassandrareaper.ReaperApplication";

  final ReaperJettyTestSupport runnerInstance;
  private SimpleReaperClient reaperClientInstance;

  public ReaperTestJettyRunner(String yamlConfigFile, Optional<String> version) {
    runnerInstance = new ReaperJettyTestSupport(Resources.getResource(yamlConfigFile).getPath(), version);
    runnerInstance.before();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (runnerInstance != null) {
          runnerInstance.after();
        }
      }
    });
  }

  public Response callReaper(String httpMethod, String urlPath, Optional<Map<String, String>> params) {
    assert runnerInstance != null : "service not initialized, call setup() first";
    return SimpleReaperClient.doHttpCall(httpMethod, "localhost", runnerInstance.getLocalPort(), urlPath, params);
  }

  public SimpleReaperClient getClient() {
    if (reaperClientInstance == null) {
      reaperClientInstance = new SimpleReaperClient("localhost", runnerInstance.getLocalPort());
    }
    return reaperClientInstance;
  }

  static final class ReaperJettyTestSupport {

    final Class<?> supportCls;
    final /*DropwizardTestSupport<ReaperApplicationConfiguration>*/Object support;

    ReaperJettyTestSupport(String configFile, Optional<String> version) {
      Class<?> appClass = getApplicationClass(version);

      try {
        // reflection for `new ReaperApplication(context)`
        Class cxtCls = appClass.getClassLoader().loadClass("io.cassandrareaper.AppContext");
        final Object application = appClass.getConstructor(cxtCls).newInstance(cxtCls.newInstance());

        // reflection for `new DropwizardTestSupport(appClass, configFile, configOverrides..)`
        supportCls = appClass.getClassLoader().loadClass("io.dropwizard.testing.DropwizardTestSupport");
        Class factoryCls = appClass.getClassLoader().loadClass("javassist.util.proxy.ProxyFactory");
        Class methodFilterCls = appClass.getClassLoader().loadClass("javassist.util.proxy.MethodFilter");
        Object factory = factoryCls.newInstance();
        factoryCls.getDeclaredMethod("setSuperclass", Class.class).invoke(factory, supportCls);

        Object methodFilter = Proxy.newProxyInstance(
            appClass.getClassLoader(),
            new Class[] {methodFilterCls},
            (Object proxy, Method method, Object[] args) -> "newApplication".equals(method.getName()));

        factoryCls.getDeclaredMethod("setFilter", methodFilterCls).invoke(factory, methodFilter);
        Class configOverrideCls = appClass.getClassLoader().loadClass("io.dropwizard.testing.ConfigOverride");
        Object cfgs = Array.newInstance(configOverrideCls, 2);
        Method config = configOverrideCls.getDeclaredMethod("config", String.class, String.class);
        Array.set(cfgs, 0, config.invoke(null, "server.adminConnectors[0].port", "" + getAnyAvailablePort()));
        Array.set(cfgs, 1, config.invoke(null, "server.applicationConnectors[0].port", "" + getAnyAvailablePort()));
        Class methodHandlerCls = appClass.getClassLoader().loadClass("javassist.util.proxy.MethodHandler");

        Object methodHandler = Proxy.newProxyInstance(
            appClass.getClassLoader(),
            new Class[] {methodHandlerCls},
            (Object proxy, Method method, Object[] args) -> application);

        support = factoryCls
            .getDeclaredMethod("create", Class[].class, Object[].class, methodHandlerCls)
            .invoke(
                factory,
                new Class[] {Class.class, String.class, cfgs.getClass()},
                new Object[] {appClass, new File(configFile).getAbsolutePath(), cfgs},
                methodHandler);

      } catch (ReflectiveOperationException ex) {
        throw new AssertionError("Failed creating the proxy to DropwizardTestSupport", ex);
      }
    }

    private Object getContext() {
      try {
        Method method = supportCls.getMethod("getApplication");
        Object application = method.invoke(support);
        Field field = application.getClass().getDeclaredField("context");
        field.setAccessible(true);
        return field.get(application);
      } catch (ReflectiveOperationException ex) {
        throw new AssertionError("Failed creating the proxy to DropwizardTestSupport", ex);
      }
    }

    static Class getApplicationClass(Optional<String> version) {
      try {
        return Class.forName(REAPER_APPLICATION_FQN);
      } catch (ClassNotFoundException ex) {
        throw new AssertionError(ex);
      }
    }

    public int getLocalPort() {
      try {
        // reflection for `dropwizardTestSupport.getLocalPort()`
        return (Integer)supportCls.getMethod("getLocalPort").invoke(support);
      } catch (ReflectiveOperationException ex) {
        throw new AssertionError(ex);
      }
    }

    public void before() {
      try {
        // reflection for `dropwizardTestSupport.before()`
        supportCls.getMethod("before").invoke(support);
      } catch (ReflectiveOperationException ex) {
        throw new AssertionError(ex);
      }
    }

    public void after() {
      try {
        Class<?> type;
        Method method;
        Object context = getContext();
        // reflection for `context.schedulingManager.cancel()`
        Field field = context.getClass().getField("schedulingManager");
        {
          Object schedulingManager = field.get(context);
          if (null != schedulingManager) {
            type = field.getType();
            method = type.getMethod("cancel");
            method.invoke(schedulingManager);
          }
        }
        {
          // reflection for `context.repairManager.close()`
          field = context.getClass().getField("repairManager");
          Object repairManager = field.get(context);
          if (null != repairManager) {
            type = field.getType();
            method = type.getDeclaredMethod("close");
            method.invoke(repairManager);
          }
        }
        {
          // reflection for `context.isRunning.set(false)`
          field = context.getClass().getField("isRunning");
          type = field.getType();
          method = type.getDeclaredMethod("set", Boolean.TYPE);
          method.invoke(field.get(context), false);
        }
        try {
          Thread.sleep(100);
          // reflection for `dropwizardTestSupport.after()`
          supportCls.getMethod("after").invoke(support);
        } catch (InterruptedException expected) { }
      } catch (ReflectiveOperationException ex) {
        throw new AssertionError(ex);
      }
    }

    String getContextStorageClassname() {
      try {
        // reflection for `context.storage.getClass().getName()`
        Object context = getContext();
        return context.getClass().getField("storage").get(context).getClass().getName();
      } catch (ReflectiveOperationException ex) {
        throw new AssertionError(ex);
      }
    }

    private static int getAnyAvailablePort() {
      // this method doesn't actually reserve the ports
      // so subsequent calls may well return the same number
      try (ServerSocket s = new ServerSocket(0)) {
        return s.getLocalPort();
      } catch (IOException ex) {
        throw new IllegalStateException("no available ports", ex);
      }
    }
  }

  static final class ParentLastURLClassLoader extends ClassLoader {

    private final ChildURLClassLoader childClassLoader;

    ParentLastURLClassLoader(final URL jarfile) throws MalformedURLException {
      super(Thread.currentThread().getContextClassLoader());
      childClassLoader = new ChildURLClassLoader(new URL("file:" + jarfile.getFile()), this.getParent());
    }

    @Override
    protected synchronized Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
      try {
        Class cls = childClassLoader.findClass(name);
        if (resolve) {
          childClassLoader.resolve(cls);
        }
        return cls;
      } catch (ClassNotFoundException e) {
        return super.loadClass(name, resolve);
      }
    }

    /**
     * This class delegates (child then parent) for the findClass method for a URLClassLoader.
     */
    static final class ChildURLClassLoader extends URLClassLoader {

      private static final URL TEST_JAR_URL;

      private final FindClassClassLoader parent;

      static {
        try {
          TEST_JAR_URL = Paths.get("test-jars", "dropwizard-testing.jar").toUri().toURL();
        } catch (MalformedURLException ex) {
          throw new AssertionError(ex);
        }
      }

      ChildURLClassLoader(final URL url, final ClassLoader parent) {
        super(new URL[] { url, TEST_JAR_URL }, null);
        this.parent = new FindClassClassLoader(parent);
      }

      @Override
      public Class<?> findClass(final String name) throws ClassNotFoundException {
        try {
          return super.findClass(name);
        } catch (ClassNotFoundException e) {
          if (name.startsWith("io.cassandrareaper.")
              // anonymous classes to ReaperApplicationConfiguration we let through
              && !name.startsWith("io.cassandrareaper.ReaperApplicationConfiguration$")) {
            throw new AssertionError(
                "ClassNotFoundException for " + name + " in shaded versioned jarfile " + super.getURLs()[0],
                e);
          }
        }
        return parent.loadClass(name);
      }

      Class resolve(Class cls) {
        super.resolveClass(cls);
        return cls;
      }

      /**
       * Make accessible `classloader.findClass(name)`
       */
      private static final class FindClassClassLoader extends ClassLoader {

        private FindClassClassLoader(final ClassLoader parent) {
          super(parent);
        }

        @Override
        public Class<?> findClass(final String name) throws ClassNotFoundException {
          return super.findClass(name);
        }
      }
    }
  }
}
