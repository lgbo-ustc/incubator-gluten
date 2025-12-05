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

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSourceV2;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunctionV2;

import io.github.zhztheplayer.velox4j.plan.PlanNode;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OffloadedJobGraphGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadedJobGraphGenerator.class);
  private final JobGraph jobGraph;
  private final ClassLoader userClassloader;
  private boolean hasGenerated = false;

  public OffloadedJobGraphGenerator(JobGraph jobGraph, ClassLoader userClassloader) {
    this.jobGraph = jobGraph;
    this.userClassloader = userClassloader;
  }

  public JobGraph generate() {
    if (hasGenerated) {
      throw new IllegalStateException("JobGraph has been generated.");
    }
    hasGenerated = true;
    for (JobVertex jobVertex : jobGraph.getVertices()) {
      offloadJobVertex(jobVertex);
    }
    return jobGraph;
  }

  private void offloadJobVertex(JobVertex jobVertex) {
    OperatorChainSegmentGenerator segmentGenerator =
        new OperatorChainSegmentGenerator(jobVertex, userClassloader);
    OperatorChainSegments segments = segmentGenerator.getSegments();
    segments.dumpLog();
    boolean x = true;

    OperatorChainSegment sourceSegment = segments.getSourceSegment();
    OperatorChainSegments targetSegments = new OperatorChainSegments();
    visitAndFoldOperatorChainSegment(sourceSegment, segments, targetSegments, 0);
    visitAndUpdateStreamEdges(sourceSegment, segments, targetSegments);
    targetSegments.dumpLog();
    if (x) {
      throw new UnsupportedOperationException("For debug");
    }

    StreamConfig sourceConfig = sourceSegment.getOperatorConfigs().get(0);
    StreamConfig targetSourceConfig =
        targetSegments.getSegment(sourceSegment.getSegmentID()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfig = new HashMap<Integer, StreamConfig>();
    if (sourceSegment.isOffloadable()) {
      sourceConfig.setStreamOperatorFactory(
          targetSourceConfig.getStreamOperatorFactory(userClassloader));
      sourceConfig.setChainedOutputs(targetSourceConfig.getChainedOutputs(userClassloader));
    } else {
      List<StreamConfig> operatorConfigs = sourceSegment.getOperatorConfigs();
      for (int i = 0; i < operatorConfigs.size(); i++) {
        StreamConfig opConfig = operatorConfigs.get(i);
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    for (OperatorChainSegment segment : targetSegments.getSegmentMap().values()) {
      if (segment.getSegmentID().equals(sourceSegment.getSegmentID())) {
        continue;
      }
      List<StreamConfig> operatorConfigs = segment.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    sourceConfig.setTransitiveChainedTaskConfigs(chainedConfig);
    sourceConfig.serializeAllConfigs();
  }

  // Fold offloadable segments
  private void visitAndFoldOperatorChainSegment(
      OperatorChainSegment segment,
      OperatorChainSegments originalSegments,
      OperatorChainSegments targetSegments,
      Integer chainedIndex) {
    List<Integer> outputs = segment.getOutputs();
    List<Integer> outputIndex = new ArrayList<>();
    OperatorChainSegment resultSegment = null;
    if (segment.isOffloadable()) {
      resultSegment = foldOffloadableSegment(segment, chainedIndex);
      chainedIndex = chainedIndex + 1;
    } else {
      resultSegment = foldUnoffloadableSegment(segment, chainedIndex);
      chainedIndex = chainedIndex + segment.getOperatorConfigs().size();
    }

    resultSegment.getInputs().addAll(segment.getInputs());
    resultSegment.getOutputs().addAll(segment.getOutputs());
    targetSegments.addSegment(segment.getSegmentID(), resultSegment);

    for (Integer outputSegIndex : outputs) {
      OperatorChainSegment outputSeg = originalSegments.getSegment(outputSegIndex);
      OperatorChainSegment outputResultSeg = targetSegments.getSegment(outputSegIndex);
      if (outputResultSeg == null) {
        visitAndFoldOperatorChainSegment(outputSeg, originalSegments, targetSegments, chainedIndex);
      }
    }
  }

  private OperatorChainSegment foldUnoffloadableSegment(
      OperatorChainSegment originalSegment, Integer chainedIndex) {
    OperatorChainSegment resultSegment = new OperatorChainSegment(originalSegment.getSegmentID());
    List<StreamConfig> operatorConfigs = originalSegment.getOperatorConfigs();
    for (StreamConfig opConfig : operatorConfigs) {
      StreamConfig newOpConfig = new StreamConfig(new Configuration(opConfig.getConfiguration()));
      newOpConfig.setChainIndex(chainedIndex);
      resultSegment.getOperatorConfigs().add(newOpConfig);
    }
    resultSegment.setOffloadable(false);
    return resultSegment;
  }

  private OperatorChainSegment foldOffloadableSegment(
      OperatorChainSegment originalSegment, Integer chainedIndex) {
    OperatorChainSegment resultSegment = new OperatorChainSegment(originalSegment.getSegmentID());
    List<StreamConfig> operatorConfigs = originalSegment.getOperatorConfigs();

    // Put all operators into a single velox plan.
    PlanNode currentPlanNode = null;
    PlanNode rootPlanNode = null;
    for (int i = operatorConfigs.size() - 1; i >= 0; i--) {
      StreamConfig opConfig = operatorConfigs.get(i);
      GlutenOperator op = getGlutenOperator(opConfig).get();
      PlanNode nextPlanNode = op.getPlanNodeV2();
      if (currentPlanNode != null) {
        currentPlanNode.setSources(List.of(nextPlanNode));
      }
      if (rootPlanNode == null) {
        rootPlanNode = currentPlanNode;
      }
      currentPlanNode = nextPlanNode;
    }

    StreamConfig sourceConfig = operatorConfigs.get(0);
    GlutenOperator sourceOp = getGlutenOperator(sourceConfig).get();
    StreamConfig rootOpConfig = operatorConfigs.get(operatorConfigs.size() - 1);
    GlutenOperator rootOp = getGlutenOperator(rootOpConfig).get();
    StreamConfig resultOpConfig =
        new StreamConfig(new Configuration(sourceConfig.getConfiguration()));

    if (sourceOp instanceof GlutenStreamSourceV2) {
      GlutenStreamSourceV2 newSourceOp =
          new GlutenStreamSourceV2(
              new GlutenSourceFunctionV2(
                  rootPlanNode,
                  rootOp.getOutputTypes(),
                  sourceOp.getId(),
                  ((GlutenStreamSourceV2) sourceOp).getConnectorSplit()));
      resultOpConfig.setStreamOperator(newSourceOp);
    } else {
      throw new UnsupportedOperationException(
          "Only GlutenStreamSourceV2 could be the root operator of an offloaded segment.");
    }

    resultOpConfig.setChainIndex(chainedIndex);
    resultSegment.getOperatorConfigs().add(resultOpConfig);
    resultSegment.setOffloadable(true);
    return resultSegment;
  }

  private StreamNode mockStreamNode(StreamConfig streamConfig) {
    return new StreamNode(
        streamConfig.getVertexID(),
        null,
        null,
        (StreamOperatorFactory<?>) streamConfig.getStreamOperatorFactory(userClassloader),
        streamConfig.getOperatorName(),
        null);
  }

  private void visitAndUpdateStreamEdges(
      OperatorChainSegment originalSegment,
      OperatorChainSegments originalSegments,
      OperatorChainSegments targetSegments) {
    OperatorChainSegment targetSegment = targetSegments.getSegment(originalSegment.getSegmentID());
    if (targetSegment.isOffloadable()) {
      List<Integer> outputIDs = originalSegment.getOutputs();
      List<StreamConfig> operatorConfigs = targetSegment.getOperatorConfigs();
      StreamConfig targetOpConfig = operatorConfigs.get(0);
      if (outputIDs.size() == 0) {
        targetOpConfig.setChainedOutputs(new ArrayList<>());
        return;
      }
      List<StreamEdge> newOutputEdges = new ArrayList<>();
      List<StreamEdge> originalOutputEdges =
          originalSegment
              .getOperatorConfigs()
              .get(originalSegment.getOperatorConfigs().size() - 1)
              .getChainedOutputs(userClassloader);
      for (int i = 0; i < outputIDs.size(); i++) {
        Integer outputID = outputIDs.get(i);
        OperatorChainSegment outputOriginalSegment = originalSegments.getSegment(outputID);
        OperatorChainSegment outputTargetSegment = targetSegments.getSegment(outputID);
        StreamConfig outputOpConfig =
            outputTargetSegment
                .getOperatorConfigs()
                .get(0); // The first operator config is the representative.
        StreamEdge originalEdge = originalOutputEdges.get(i);
        StreamEdge newEdge =
            new StreamEdge(
                mockStreamNode(targetOpConfig),
                mockStreamNode(outputOpConfig),
                originalEdge.getTypeNumber(),
                originalEdge.getBufferTimeout(),
                originalEdge.getPartitioner(),
                originalEdge.getOutputTag(),
                originalEdge.getExchangeMode(),
                0,
                originalEdge.getIntermediateDatasetIdToProduce());
        newOutputEdges.add(newEdge);
      }
      targetOpConfig.setChainedOutputs(newOutputEdges);
    }
  }

  private Optional<GlutenOperator> getGlutenOperator(StreamConfig taskConfig) {
    StreamOperatorFactory operatorFactory = taskConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      StreamOperator streamOperator = taskConfig.getStreamOperator(userClassloader);
      if (streamOperator instanceof GlutenOperator) {
        return Optional.of((GlutenOperator) streamOperator);
      }
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return Optional.of(((GlutenOneInputOperatorFactory) operatorFactory).getOperator());
    }
    return Optional.empty();
  }
}
