/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperatorChainSegments {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorChainSegments.class);
  private Map<Integer, OperatorChainSegment> segmentMap;

  public OperatorChainSegments() {
    segmentMap = new HashMap<>();
  }

  public void addSegment(Integer id, OperatorChainSegment segment) {
    segmentMap.put(id, segment);
  }

  public OperatorChainSegment getSegment(Integer id) {
    return segmentMap.get(id);
  }

  public void removeSegment(Integer id) {
    segmentMap.remove(id);
  }

  public OperatorChainSegment getSourceSegment() {
    List<OperatorChainSegment> sourceCandidates = new ArrayList<>();

    for (OperatorChainSegment segment : segmentMap.values()) {
      if (segment.getInputs().isEmpty()) {
        sourceCandidates.add(segment);
      }
    }

    if (sourceCandidates.isEmpty()) {
      throw new IllegalStateException("No source segment found (no segment with empty inputs)");
    } else if (sourceCandidates.size() > 1) {
      throw new IllegalStateException(
          "Multiple source segments found: "
              + sourceCandidates.size()
              + " segments have empty inputs");
    }

    return sourceCandidates.get(0);
  }

  public Map<Integer, OperatorChainSegment> getSegmentMap() {
    return segmentMap;
  }

  public void dumpLog() {
    for (OperatorChainSegment segment : segmentMap.values()) {
      LOG.info("Segment ID: {}, offloadable: {}", segment.getSegmentID(), segment.isOffloadable());
      LOG.info("  Inputs: {}", segment.getInputs().toString());
      LOG.info("  Outputs: {}", segment.getOutputs().toString());
      LOG.info(
          "  Operator Configs: {}",
          segment.getOperatorConfigs().stream()
              .map(config -> config.getOperatorName() + "(" + config.getVertexID() + ")")
              .reduce((a, b) -> a + ", " + b)
              .orElse(""));
    }
  }
}
