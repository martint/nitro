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
package org.weakref.nitro.data;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Classloader-neutral description of one row-local evaluation failure.
 *
 * <p>The namespace and code are provider-defined. Boundary adapters translate
 * them to host exceptions without retaining a provider exception or class.
 */
public record ErrorValue(
        String namespace,
        int code,
        String name,
        String type,
        boolean fatal,
        String message,
        List<StackTraceElement> stackTrace)
{
    public ErrorValue
    {
        namespace = requireNonNull(namespace, "namespace is null");
        name = requireNonNull(name, "name is null");
        type = requireNonNull(type, "type is null");
        message = requireNonNull(message, "message is null");
        stackTrace = List.copyOf(requireNonNull(stackTrace, "stackTrace is null"));
    }

    public ErrorValue(String namespace, int code, String name, String type, String message)
    {
        this(namespace, code, name, type, false, message, List.of());
    }

    public ErrorValue(String namespace, int code, String name, String type, boolean fatal, String message)
    {
        this(namespace, code, name, type, fatal, message, List.of());
    }
}
