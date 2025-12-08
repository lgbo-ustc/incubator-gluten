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
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperatorV2;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunctionV2;
import org.apache.gluten.table.runtime.typeutils.GlutenRowVectorRefSerializer;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;

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
      StreamConfig config = new StreamConfig(jobVertex.getConfiguration());
      Map<Integer, StreamConfig> chainedConfigs =
          config.getTransitiveChainedTaskConfigsWithSelf(userClassloader);
      LOG.error(
          "xxx after offload op {}, transived chainedConfigs: {}",
          config.getOperatorName(),
          chainedConfigs.size());
      chainedConfigs = config.getTransitiveChainedTaskConfigs(userClassloader);
      LOG.error(
          "xxx after offload op {}, chainedConfigs: {}",
          config.getOperatorName(),
          chainedConfigs.size());
    }
    return jobGraph;
  }

  private void offloadJobVertex(JobVertex jobVertex) {
    OperatorChainSegmentGenerator segmentGenerator =
        new OperatorChainSegmentGenerator(jobVertex, userClassloader);
    OperatorChainSegments segments = segmentGenerator.getSegments();
    segments.dumpLog();
    boolean x = false;

    OperatorChainSegment sourceSegment = segments.getSourceSegment();
    OperatorChainSegments targetSegments = new OperatorChainSegments();
    visitAndFoldOperatorChainSegment(sourceSegment, segments, targetSegments, 0);
    visitAndUpdateStreamEdges(sourceSegment, segments, targetSegments);
    serializeAllOperatorsConfigs(targetSegments);
    targetSegments.dumpLog();
    if (x) {
      throw new UnsupportedOperationException("For debug");
    }

    StreamConfig sourceConfig = sourceSegment.getOperatorConfigs().get(0);
    StreamConfig targetSourceConfig =
        targetSegments.getSegment(sourceSegment.getSegmentID()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfig = new HashMap<Integer, StreamConfig>();
    if (sourceSegment.isOffloadable()) {
      LOG.error("xxx offload job vertex: {}", sourceConfig.getOperatorName());
      sourceConfig.setStreamOperatorFactory(
          targetSourceConfig.getStreamOperatorFactory(userClassloader));
      List<StreamEdge> chainedOutputs = targetSourceConfig.getChainedOutputs(userClassloader);
      LOG.error("xxx source has {} outputs", chainedOutputs.size());
      sourceConfig.setChainedOutputs(targetSourceConfig.getChainedOutputs(userClassloader));
      LOG.error(
          "xxx type serializer: {}",
          targetSourceConfig.getTypeSerializerOut(userClassloader).getClass().getName());
      sourceConfig.setTypeSerializerOut(targetSourceConfig.getTypeSerializerOut(userClassloader));
    } else {
      List<StreamConfig> operatorConfigs = sourceSegment.getOperatorConfigs();
      for (int i = 0; i < operatorConfigs.size(); i++) {
        StreamConfig opConfig = operatorConfigs.get(i);
        LOG.error("xxx add chained config 1: {}", opConfig.getOperatorName());
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    for (OperatorChainSegment segment : targetSegments.getSegmentMap().values()) {
      if (segment.getSegmentID().equals(sourceSegment.getSegmentID())) {
        continue;
      }
      List<StreamConfig> operatorConfigs = segment.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        LOG.error("xxx add chained config: {}", opConfig.getOperatorName());
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    LOG.error("xxx total chained configs: {}", chainedConfig.size());
    sourceConfig.setTransitiveChainedTaskConfigs(chainedConfig);
    sourceConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfig);
    LOG.error(
        "xxxx after offload op {}. {}",
        sourceConfig.getOperatorName(),
        sourceConfig.getTransitiveChainedTaskConfigs(userClassloader).size());
    sourceConfig.serializeAllConfigs();
    Map<Integer, StreamConfig> testChainedConfigs =
        sourceConfig.getTransitiveChainedTaskConfigsWithSelf(userClassloader);
    LOG.error(
        "xxx after offload op {}, testChainedConfigs: {}",
        sourceConfig.getOperatorName(),
        testChainedConfigs.size());
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
      resultSegment = foldOffloadableSegment(originalSegments, segment, chainedIndex);
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
      OperatorChainSegments segments, OperatorChainSegment originalSegment, Integer chainedIndex) {
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
    // There is only one operator in this segment.
    if (rootPlanNode == null) {
      rootPlanNode = getGlutenOperator(operatorConfigs.get(0)).get().getPlanNodeV2();
    }

    StreamConfig sourceConfig = operatorConfigs.get(0);
    GlutenOperator sourceOp = getGlutenOperator(sourceConfig).get();
    StreamConfig rootOpConfig = operatorConfigs.get(operatorConfigs.size() - 1);
    GlutenOperator rootOp = getGlutenOperator(rootOpConfig).get();
    StreamConfig resultOpConfig =
        new StreamConfig(new Configuration(sourceConfig.getConfiguration()));

    if (sourceOp instanceof GlutenStreamSourceV2) {
      boolean couldOutputRowVector = couldOutputRowVector(originalSegment, segments);
      Class<?> outClass = couldOutputRowVector ? RowVector.class : RowData.class;
      GlutenStreamSourceV2 newSourceOp =
          new GlutenStreamSourceV2(
              new GlutenSourceFunctionV2<>(
                  rootPlanNode,
                  rootOp.getOutputTypes(),
                  sourceOp.getId(),
                  ((GlutenStreamSourceV2) sourceOp).getConnectorSplit(),
                  outClass));
      resultOpConfig.setStreamOperator(newSourceOp);
      if (couldOutputRowVector) {
        LOG.error(
            "xxx op: {} use self-defined RowVector serializer", rootOpConfig.getOperatorName());
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        resultOpConfig.setTypeSerializerOut(new GlutenRowVectorRefSerializer(rowType));
      }

    } else if (sourceOp instanceof GlutenOneInputOperatorV2) {
      LOG.error("xxx fold GlutenOneInputOperatorV2");
      boolean couldOutputRowVector = couldOutputRowVector(originalSegment, segments);
      boolean couldInputRowVector = couldInputRowVector(originalSegment, segments);
      Class<?> inClass = couldInputRowVector ? RowVector.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? RowVector.class : RowData.class;
      LOG.error(
          "xxx GlutenOneInputOperatorV2. inClass: {}, outClass: {}",
          inClass.getName(),
          outClass.getName());
      GlutenOneInputOperatorV2 newOneInputOp =
          new GlutenOneInputOperatorV2(
              rootPlanNode,
              sourceOp.getId(),
              sourceOp.getInputType(),
              rootOp.getOutputTypes(),
              inClass,
              outClass);
      resultOpConfig.setStreamOperator(newOneInputOp);
      if (couldOutputRowVector) {
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        resultOpConfig.setTypeSerializerOut(new GlutenRowVectorRefSerializer(rowType));
      }
      if (couldInputRowVector) {
        resultOpConfig.setupNetworkInputs(
            new GlutenRowVectorRefSerializer(sourceOp.getInputType()));
      }
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
    LOG.error("xxx visitAndUpdateStreamEdges for segment {}", targetSegment.getSegmentID());
    if (targetSegment.isOffloadable()) {
      List<Integer> outputIDs = originalSegment.getOutputs();
      List<StreamConfig> operatorConfigs = targetSegment.getOperatorConfigs();
      StreamConfig targetOpConfig = operatorConfigs.get(0);
      LOG.error("xxx visitAndUpdateStreamEdges. op: {}", targetOpConfig.getOperatorName());
      if (outputIDs.size() == 0) {
        targetOpConfig.setChainedOutputs(new ArrayList<>());
        LOG.error(
            "xxx op {} has no outputs. {}",
            targetOpConfig.getOperatorName(),
            targetOpConfig.getChainedOutputs(userClassloader).size());
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
      LOG.error(
          "xxx op {} set {} outputs", targetOpConfig.getOperatorName(), newOutputEdges.size());
      targetOpConfig.setChainedOutputs(newOutputEdges);
    }

    for (Integer outputSeg : originalSegment.getOutputs()) {
      visitAndUpdateStreamEdges(
          originalSegments.getSegment(outputSeg), originalSegments, targetSegments);
    }
  }

  void serializeAllOperatorsConfigs(OperatorChainSegments segments) {
    for (OperatorChainSegment segment : segments.getSegmentMap().values()) {
      List<StreamConfig> operatorConfigs = segment.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        opConfig.serializeAllConfigs();
      }
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

  boolean isAllOffloadable(OperatorChainSegments segments, List<Integer> segmentIDs) {
    for (Integer segmentID : segmentIDs) {
      OperatorChainSegment segment = segments.getSegment(segmentID);
      if (!segment.isOffloadable()) {
        return false;
      }
    }
    return true;
  }

  boolean couldOutputRowVector(OperatorChainSegment segment, OperatorChainSegments segments) {
    boolean could = true;
    for (Integer outputID : segment.getOutputs()) {
      OperatorChainSegment outputSegment = segments.getSegment(outputID);
      if (!outputSegment.isOffloadable()) {
        could = false;
        break;
      }
      List<Integer> inputs = outputSegment.getInputs();
      if (!isAllOffloadable(segments, inputs)) {
        could = false;
        break;
      }
    }
    return could;
  }

  boolean couldInputRowVector(OperatorChainSegment segment, OperatorChainSegments segments) {
    boolean could = true;
    for (Integer inputID : segment.getInputs()) {
      OperatorChainSegment inputSegment = segments.getSegment(inputID);
      if (!inputSegment.isOffloadable()) {
        could = false;
        break;
      }
      if (!couldOutputRowVector(inputSegment, segments)) {
        could = false;
        break;
      }
    }
    return could;
  }
}
