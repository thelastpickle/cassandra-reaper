/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

import io.cassandrareaper.ReaperApplication;
import io.cassandrareaper.acceptance.ReaperTestJettyRunner.ParentLastURLClassLoader.ChildURLClassLoader;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.fest.assertions.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ChildURLClassLoaderTest {

  private static final Logger LOG = LoggerFactory.getLogger(ChildURLClassLoaderTest.class);

  @Test
  public void test_versions_classloader() throws Exception {

    int tested = 0;
    String[] versions = System.getProperty("cucumber.versions-to-test.original").split("\n");
    for (String line : versions) {
      if (!(line.contains("Examples") || line.contains("version"))) {
        String version = line.replace("|", "").trim();

        if (!version.isEmpty()) {
          URL url = Paths.get("test-jars", String.format("cassandra-reaper-%s.jar", version)).toUri().toURL();

          LOG.error("Testing classloading with " + url);

          ChildURLClassLoader childClassLoader
              = new ChildURLClassLoader(new URL[]{new URL("jar:file:" + url.getFile() + "!/")}, null);

          Assertions
              .assertThat(childClassLoader.loadClass(ReaperApplication.class.getName()))
              .isNotNull();

          list(url.getFile());
          ++tested;
        }
      }
    }
    Assertions.assertThat(tested).isGreaterThan(0);
  }

  private static void list(String pathToJar) throws IOException, ClassNotFoundException {
    JarFile jarFile = new JarFile(pathToJar);
    URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
    URLClassLoader cl = URLClassLoader.newInstance(urls);

    for (JarEntry entry : Collections.list(jarFile.entries())) {
      if (entry.isDirectory()
          || !entry.getName().endsWith(".class")
          || !entry.getName().startsWith("io/cassandrareaper/")) {
        continue;
      }
      // -6 because of ".class"
      String className = entry.getName().substring(0, entry.getName().length() - 6).replace('/', '.');
      LOG.info(pathToJar + "!/" + className);
      cl.loadClass(className);
    }
  }
}
