/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.ecosystem.io.lakehouse.common;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtobufSchemaHelper {

  public static List<Descriptor> parseFileDescriptorSet(FileDescriptorSet fileDescriptorSet,
      String rootFileName) {
    Map<String, Descriptors.FileDescriptor> loadedFileDescriptors = new HashMap<>();
    Map<String, FileDescriptorProto> availableFileDescriptors = fileDescriptorSet.getFileList()
        .stream()
        .collect(Collectors.toMap(FileDescriptorProto::getName, v -> v));

    MutableGraph<String> depGraph = GraphBuilder.directed().build();
    for (FileDescriptorProto fileDescriptorProto : fileDescriptorSet.getFileList()) {
      depGraph.addNode(fileDescriptorProto.getName());
      fileDescriptorProto.getDependencyList()
          .forEach(dep -> depGraph.putEdge(fileDescriptorProto.getName(), dep));
    }
    if (depGraph.nodes().contains(rootFileName)) {
      Traverser.forGraph(depGraph).depthFirstPostOrder(rootFileName).forEach(fdName -> {
        log.debug("Loading Protobuf Dependency: " + fdName);
        FileDescriptorProto fdp = availableFileDescriptors.get(fdName);
        FileDescriptor fd = loadFileDescriptor(fdp, loadedFileDescriptors);
        Optional.ofNullable(fd).ifPresent(f -> loadedFileDescriptors.put(f.getName(), f));
      });
    }
    return loadedFileDescriptors.values().stream()
        .map(FileDescriptor::getMessageTypes)
        .flatMap(Collection::stream)
        .distinct()
        .collect(Collectors.toList());
  }

  // Recursively load file descriptors and resolve dependencies
  private static Descriptors.FileDescriptor loadFileDescriptor(
      DescriptorProtos.FileDescriptorProto fileDescriptorProto,
      Map<String, Descriptors.FileDescriptor> loadedFileDescriptors) {
    // Check if the file descriptor is already loaded
    if (loadedFileDescriptors.containsKey(fileDescriptorProto.getName())) {
      return loadedFileDescriptors.get(fileDescriptorProto.getName());
    }

    // Load dependencies first
    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>(loadedFileDescriptors.values());
    for (String dependencyName : fileDescriptorProto.getDependencyList()) {
      DescriptorProtos.FileDescriptorProto dependencyProto = findDependencyProto(dependencyName,
          loadedFileDescriptors);
      dependencies.add(loadFileDescriptor(dependencyProto, loadedFileDescriptors));
    }

    try {
      // Reconstruct the original descriptor
      return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto,
          dependencies.toArray(new Descriptors.FileDescriptor[0]));
    } catch (Descriptors.DescriptorValidationException e) {
      log.error("Error loading file descriptor: {}", fileDescriptorProto.getName(), e);
      throw new RuntimeException(e);
    }
  }

  // Find a dependency by name in the loaded file descriptors
  private static DescriptorProtos.FileDescriptorProto findDependencyProto(String dependencyName,
      Map<String, Descriptors.FileDescriptor> loadedFileDescriptors) {
    for (Descriptors.FileDescriptor fileDescriptor : loadedFileDescriptors.values()) {
      if (fileDescriptor.getName().equals(dependencyName)) {
        return fileDescriptor.toProto();
      }
    }
    throw new IllegalArgumentException("Dependency not found: " + dependencyName);
  }

}
