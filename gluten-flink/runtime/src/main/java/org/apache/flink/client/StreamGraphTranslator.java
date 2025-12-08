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
package org.apache.flink.client;

import org.apache.gluten.client.OffloadedJobGraphGenerator;
import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.streaming.api.operators.GlutenStreamSourceV2;
import org.apache.gluten.table.runtime.keyselector.GlutenKeySelector;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunctionV2;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenVectorSourceFunction;
import org.apache.gluten.table.runtime.operators.GlutenVectorTwoInputOperator;
import org.apache.gluten.table.runtime.typeutils.GlutenRowVectorSerializer;
import org.apache.gluten.util.Utils;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
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

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkPipelineTranslator} for DataStream API {@link StreamGraph StreamGraphs}.
 *
 * <p>Note: this is used through reflection in {@link
 * org.apache.flink.client.FlinkPipelineTranslationUtil}.
 */
@SuppressWarnings("unused")
public class StreamGraphTranslator implements FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(StreamGraphTranslator.class);

  private final ClassLoader userClassloader;

  public StreamGraphTranslator(ClassLoader userClassloader) {
    this.userClassloader = userClassloader;
  }

  @Override
  public JobGraph translateToJobGraph(
      Pipeline pipeline, Configuration optimizerConfiguration, int defaultParallelism) {
    checkArgument(
        pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

    StreamGraph streamGraph = (StreamGraph) pipeline;
    JobGraph jobGraph = streamGraph.getJobGraph(userClassloader, null);
    // return mergeGlutenOperators(jobGraph);
    // return wrapGlutenTasks(jobGraph);
    OffloadedJobGraphGenerator generator =
        new OffloadedJobGraphGenerator(jobGraph, userClassloader);
    return generator.generate();
  }

  @Override
  public String translateToJSONExecutionPlan(Pipeline pipeline) {
    checkArgument(
        pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

    StreamGraph streamGraph = (StreamGraph) pipeline;

    return streamGraph.getStreamingPlanAsJSON();
  }

  @Override
  public boolean canTranslate(Pipeline pipeline) {
    return pipeline instanceof StreamGraph;
  }

  // --- Begin Gluten-specific code changes ---
  private JobGraph mergeGlutenOperators(JobGraph jobGraph) {
    for (JobVertex vertex : jobGraph.getVertices()) {
      StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
      buildGlutenChains(streamConfig);
      LOG.debug("Vertex {} is {}.", vertex.getName(), streamConfig);
    }
    return jobGraph;
  }

  // A JobVertex may contain several operators chained like this: Source-->Op1-->Op2-->Sink1.
  //                                                                                -->Sink2.
  // If the operators connected all support translated to gluten, we merge them into
  // a single GlutenOperator to avoid data transferred between flink and native.
  // One operator may be followed by several other operators.
  private void buildGlutenChains(StreamConfig vertexConfig) {
    Map<Integer, StreamConfig> serializedTasks =
        vertexConfig.getTransitiveChainedTaskConfigs(userClassloader);
    Map<Integer, StreamConfig> chainedTasks = new HashMap<>(serializedTasks.size());
    serializedTasks.forEach(
        (id, config) -> chainedTasks.put(id, new StreamConfig(config.getConfiguration())));
    buildGlutenChains(vertexConfig, chainedTasks);
    // TODO: may need fallback if failed.
    vertexConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedTasks);
  }

  private void buildGlutenChains(StreamConfig taskConfig, Map<Integer, StreamConfig> chainedTasks) {
    List<StreamEdge> outEdges = taskConfig.getChainedOutputs(userClassloader);
    Optional<GlutenOperator> sourceOperatorOpt = getGlutenOperator(taskConfig);
    GlutenOperator sourceOperator = sourceOperatorOpt.orElse(null);
    boolean isSourceGluten = sourceOperatorOpt.isPresent();
    if (outEdges == null || outEdges.isEmpty()) {
      LOG.debug("{} has no chained task.", taskConfig.getOperatorName());
      // TODO: judge whether can set?
      if (isSourceGluten) {
        if (taskConfig.getOperatorName().equals("exchange-hash")) {
          taskConfig.setTypeSerializerOut(new GlutenRowVectorSerializer(null));
        }
        Map<IntermediateDataSetID, String> nodeToNonChainedOuts = new HashMap<>(outEdges.size());
        taskConfig
            .getOperatorNonChainedOutputs(userClassloader)
            .forEach(edge -> nodeToNonChainedOuts.put(edge.getDataSetId(), sourceOperator.getId()));
        Utils.setNodeToNonChainedOutputs(taskConfig, nodeToNonChainedOuts);
        taskConfig.serializeAllConfigs();
      }
      return;
    }
    Map<String, Integer> nodeToChainedOuts = new HashMap<>(outEdges.size());
    Map<IntermediateDataSetID, String> nodeToNonChainedOuts = new HashMap<>(outEdges.size());
    Map<String, RowType> nodeToOutTypes = new HashMap<>(outEdges.size());
    List<StreamEdge> chainedOutputs = new ArrayList<>(outEdges.size());
    List<NonChainedOutput> nonChainedOutputs = new ArrayList<>(outEdges.size());
    StatefulPlanNode sourceNode = isSourceGluten ? sourceOperator.getPlanNode() : null;
    boolean allGluten = true;
    LOG.debug("Edge size {}, OP {}", outEdges.size(), sourceOperator);
    for (StreamEdge outEdge : outEdges) {
      StreamConfig outTask = chainedTasks.get(outEdge.getTargetId());
      if (outTask == null) {
        LOG.error("Not find task {} in Chained tasks", outEdge.getTargetId());
        allGluten = false;
        break;
      }
      buildGlutenChains(outTask, chainedTasks);
      Optional<GlutenOperator> outOperator = getGlutenOperator(outTask);
      if (isSourceGluten && outOperator.isPresent()) {
        StatefulPlanNode outNode = outOperator.get().getPlanNode();
        if (sourceNode != null) {
          sourceNode.addTarget(outNode);
          LOG.debug("Add {} target {}", sourceNode, outNode);
        } else {
          sourceNode = outNode;
          LOG.debug("Set target node to {}", outNode);
        }
        Map<String, Integer> node2Out = Utils.getNodeToChainedOutputs(outTask, userClassloader);
        if (node2Out != null) {
          nodeToChainedOuts.putAll(node2Out);
          nodeToNonChainedOuts.putAll(Utils.getNodeToNonChainedOutputs(outTask, userClassloader));
        } else {
          outTask
              .getChainedOutputs(userClassloader)
              .forEach(edge -> nodeToChainedOuts.put(outNode.getId(), edge.getTargetId()));
          outTask
              .getOperatorNonChainedOutputs(userClassloader)
              .forEach(edge -> nodeToNonChainedOuts.put(edge.getDataSetId(), outNode.getId()));
        }
        nodeToOutTypes.putAll(outOperator.get().getOutputTypes());
        chainedOutputs.addAll(outTask.getChainedOutputs(userClassloader));
        nonChainedOutputs.addAll(outTask.getOperatorNonChainedOutputs(userClassloader));
      } else {
        allGluten = false;
        LOG.debug(
            "{} and {} can not be merged", taskConfig.getOperatorName(), outTask.getOperatorName());
        break;
      }
    }
    if (allGluten) {
      if (sourceOperator instanceof GlutenStreamSource) {
        GlutenStreamSource streamSource = (GlutenStreamSource) sourceOperator;
        taskConfig.setStreamOperator(
            new GlutenStreamSource(
                new GlutenVectorSourceFunction(
                    sourceNode,
                    nodeToOutTypes,
                    sourceOperator.getId(),
                    streamSource.getConnectorSplit())));
      } else if (sourceOperator instanceof GlutenVectorTwoInputOperator) {
        GlutenVectorTwoInputOperator twoInputOperator =
            (GlutenVectorTwoInputOperator) sourceOperator;
        taskConfig.setStreamOperator(
            new GlutenVectorTwoInputOperator(
                sourceNode,
                twoInputOperator.getLeftId(),
                twoInputOperator.getRightId(),
                twoInputOperator.getLeftInputType(),
                twoInputOperator.getRightInputType(),
                nodeToOutTypes));
        // TODO: judge whether can set?
        taskConfig.setStatePartitioner(0, new GlutenKeySelector());
        taskConfig.setStatePartitioner(1, new GlutenKeySelector());
        taskConfig.setupNetworkInputs(
            new GlutenRowVectorSerializer(null), new GlutenRowVectorSerializer(null));
      } else {
        taskConfig.setStreamOperator(
            new GlutenVectorOneInputOperator(
                sourceNode, sourceOperator.getId(), sourceOperator.getInputType(), nodeToOutTypes));
        // TODO: judge whether can set?
        taskConfig.setStatePartitioner(0, new GlutenKeySelector());
        taskConfig.setupNetworkInputs(new GlutenRowVectorSerializer(null));
      }
      Utils.setNodeToChainedOutputs(taskConfig, nodeToChainedOuts);
      Utils.setNodeToNonChainedOutputs(taskConfig, nodeToNonChainedOuts);
      taskConfig.setChainedOutputs(chainedOutputs);
      taskConfig.setOperatorNonChainedOutputs(nonChainedOutputs);
      taskConfig.serializeAllConfigs();
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

  private class PlanNodeWithSources {
    public PlanNode node;
    public List<PlanNode> sources;
    Map<String, RowType> outTypes;
  }
  // If there is any non-gluten operator in the chained operators, return false.
  // And we will not offload this chain to velox.
  private boolean isAllGlutendOperators(StreamConfig config) {
    Optional<GlutenOperator> op = getGlutenOperator(config);
    if (!op.isPresent()) {
      return false;
    }
    Map<Integer, StreamConfig> chainedTasks =
        config.getTransitiveChainedTaskConfigs(userClassloader);
    for (StreamConfig c : chainedTasks.values()) {
      op = getGlutenOperator(c);
      if (!op.isPresent()) {
        return false;
      }
    }
    return true;
  }

  private JobGraph wrapGlutenTasks(JobGraph jobGraph) {
    for (JobVertex vertex : jobGraph.getVertices()) {
      offloadOperators(vertex);
    }
    return jobGraph;
  }

  // Try to offload operators into velox.
  private void offloadOperators(JobVertex vertex) {
    // It's neccessary to new a StreamConfig. Otherwise, the transient will null.
    // After update chained tasks, we need to serialize them back(call
    // config.serializeAllConfigs()).
    StreamConfig config = new StreamConfig(vertex.getConfiguration());
    StreamConfig configTest = new StreamConfig(new Configuration(config.getConfiguration()));
    configTest.setChainIndex(111);
    LOG.error("xxx config chained index: {}", config.getChainIndex());

    if (!isAllGlutendOperators(config)) {
      // TODO: fallback to flink execution for this whole operator chain, make things simple.
      throw new UnsupportedOperationException("There is non-gluten operator in the chain.");
    }
    Map<Integer, PlanNodeWithSources> builtPlanNodes = new HashMap<>();
    Map<Integer, PlanNodeWithSources> leafPlanNodes = new HashMap<>();
    PlanNodeWithSources endPlanNode = null;
    GlutenOperator sourceOp = getGlutenOperator(config).get();
    StreamConfig nextOpConfig = null;
    Integer nextOpId = null;

    Map<Integer, StreamConfig> chainedTasks = new HashMap<>();
    config
        .getTransitiveChainedTaskConfigs(userClassloader)
        .forEach((id, conf) -> chainedTasks.put(id, new StreamConfig(conf.getConfiguration())));

    endPlanNode = coalesceGlutenOperators(config, chainedTasks, builtPlanNodes, leafPlanNodes);

    // Set plan node sources.
    for (Map.Entry<Integer, PlanNodeWithSources> entry : builtPlanNodes.entrySet()) {
      if (entry.getValue().sources.size() > 0) {
        entry.getValue().node.setSources(entry.getValue().sources);
      }
    }

    // TODO
    if (leafPlanNodes.size() != 1) {
      throw new UnsupportedOperationException(
          "GlutenOperator chain with multiple leaf nodes is not supported yet.");
    }

    Map<Integer, StreamConfig> chainedTasksConfig = new HashMap<Integer, StreamConfig>();
    if (sourceOp instanceof GlutenStreamSourceV2) {
      PlanNodeWithSources leafNode = leafPlanNodes.values().iterator().next();
      PlanNode leafPlanNode = leafNode.node;

      GlutenStreamSourceV2 newSourceOp =
          new GlutenStreamSourceV2(
              new GlutenSourceFunctionV2<RowData>(
                  leafPlanNode,
                  leafNode.outTypes,
                  sourceOp.getId(),
                  ((GlutenStreamSourceV2) sourceOp).getConnectorSplit(),
                  RowData.class));

      RowType outpuType = leafNode.outTypes.entrySet().iterator().next().getValue();

      config.setStreamOperator(newSourceOp);

      // This node is the only node in the chain. There is no downstream nodes.
      List<StreamEdge> newOutEdges = new ArrayList<>();
      config.setChainedOutputs(newOutEdges);
    } else {
      throw new UnsupportedOperationException(
          "GlutenOperator chain with non-source root node is not supported yet.");
    }

    // Update chained tasks config.
    config.setTransitiveChainedTaskConfigs(chainedTasksConfig);
    // this must be called to update the config.
    config.serializeAllConfigs();
  }

  private PlanNodeWithSources coalesceGlutenOperators(
      StreamConfig taskConfig,
      Map<Integer, StreamConfig> chainedTasks,
      Map<Integer, PlanNodeWithSources> builtPlanNodes,
      Map<Integer, PlanNodeWithSources> leafPlanNodes) {
    GlutenOperator operator = getGlutenOperator(taskConfig).get();
    PlanNodeWithSources planNode = builtPlanNodes.get(taskConfig.getChainIndex());
    if (planNode == null) {
      planNode = new PlanNodeWithSources();
      planNode.node = operator.getPlanNodeV2();
      planNode.sources = new ArrayList<>();
      planNode.outTypes = operator.getOutputTypes();
      builtPlanNodes.put(taskConfig.getChainIndex(), planNode);

      List<StreamEdge> outEdges = taskConfig.getChainedOutputs(userClassloader);
      if (outEdges == null || outEdges.isEmpty()) {
        // No outEdges means currentOperator is the tail of the chain.
        leafPlanNodes.put(taskConfig.getChainIndex(), planNode);
      } else if (outEdges.size() == 1) {
        StreamConfig nextTaskConfig = chainedTasks.get(outEdges.get(0).getTargetId());

        if (nextTaskConfig == null) {
          throw new IllegalStateException(
              "Not find task " + outEdges.get(0).getTargetId() + " in Chained tasks");
        }
        PlanNodeWithSources nextPlanNode =
            coalesceGlutenOperators(nextTaskConfig, chainedTasks, builtPlanNodes, leafPlanNodes);
        if (nextPlanNode.sources.size() > 0) {
          // TODO: It has multiple sources.
          throw new UnsupportedOperationException(
              "GlutenOperator coalesce does not support multiple sources yet.");
        }
        nextPlanNode.sources.add(planNode.node);
      } else {
        // TODO: It has multiple outputs.
        throw new UnsupportedOperationException(
            "GlutenOperator coalesce does not support multiple outputs yet.");
      }
    }
    return planNode;
  }

  // --- End Gluten-specific code changes ---
}
