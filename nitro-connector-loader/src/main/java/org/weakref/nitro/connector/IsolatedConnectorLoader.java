/*
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
package org.weakref.nitro.connector;

import org.weakref.nitro.core.connector.ConnectorFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;

/**
 * Instance-owned discovery for one isolated connector class path.
 *
 * <p>Only JDK classes and the Nitro SPI/data artifact are delegated to the host. Provider implementation and
 * dependency classes must come from the supplied connector class path.
 */
public final class IsolatedConnectorLoader
        implements AutoCloseable
{
    private final ConnectorClassLoader classLoader;
    private boolean closed;

    public IsolatedConnectorLoader(List<URL> classPath)
    {
        requireNonNull(classPath, "classPath is null");
        this.classLoader = new ConnectorClassLoader(
                classPath.toArray(URL[]::new),
                ConnectorFactory.class.getClassLoader());
    }

    public ConnectorFactory load(String name)
    {
        requireNonNull(name, "name is null");
        if (closed) {
            throw new IllegalStateException("connector loader is closed");
        }

        ConnectorFactory match = null;
        for (ConnectorFactory factory : ServiceLoader.load(ConnectorFactory.class, classLoader)) {
            if (!factory.name().equals(name)) {
                continue;
            }
            if (match != null) {
                throw new IllegalStateException("multiple connector factories named: " + name);
            }
            match = factory;
        }
        if (match == null) {
            throw new IllegalArgumentException("connector factory not found: " + name);
        }
        return match;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            classLoader.close();
        }
        catch (IOException exception) {
            throw new RuntimeException("failed to close connector class loader", exception);
        }
    }

    private static final class ConnectorClassLoader
            extends URLClassLoader
    {
        private ConnectorClassLoader(URL[] urls, ClassLoader spiClassLoader)
        {
            super(urls, spiClassLoader);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException
        {
            if (isHostClass(name)) {
                return super.loadClass(name, resolve);
            }

            synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                    type = findClass(name);
                }
                if (resolve) {
                    resolveClass(type);
                }
                return type;
            }
        }

        @Override
        public Enumeration<URL> getResources(String name)
                throws IOException
        {
            if (name.startsWith("META-INF/services/")) {
                return findResources(name);
            }
            return super.getResources(name);
        }

        private static boolean isHostClass(String name)
        {
            return name.startsWith("java.") ||
                    name.startsWith("jdk.") ||
                    name.startsWith("sun.") ||
                    name.startsWith("org.weakref.nitro.core.") ||
                    name.startsWith("org.weakref.nitro.data.");
        }
    }
}
