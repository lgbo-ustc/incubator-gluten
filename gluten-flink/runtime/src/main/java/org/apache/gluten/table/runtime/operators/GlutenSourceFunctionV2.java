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
package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.table.runtime.config.VeloxQueryConfig;
import org.apache.gluten.table.runtime.metrics.SourceTaskMetrics;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Gluten legacy source function, call velox plan to execute. It sends RowVector to downstream
 * instead of RowData to avoid data convert.
 */
public class GlutenSourceFunctionV2 extends RichParallelSourceFunction<RowData>
    implements CheckpointedFunction, CheckpointListener {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenSourceFunctionV2.class);

  private final PlanNode planNode;
  private final Map<String, RowType> outputTypes;
  private final String id;
  private final ConnectorSplit split;
  private volatile boolean isRunning = true;

  private Session session;
  private Query query;
  private BufferAllocator allocator;
  private MemoryManager memoryManager;
  private SerialTask task;
  private SourceTaskMetrics taskMetrics;

  public GlutenSourceFunctionV2(
      PlanNode planNode, Map<String, RowType> outputTypes, String id, ConnectorSplit split) {
    this.planNode = planNode;
    this.outputTypes = outputTypes;
    this.id = id;
    this.split = split;
  }

  public StatefulPlanNode getPlanNode() {
    return null;
  }

  public PlanNode getPlanNodeV2() {
    return planNode;
  }

  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  public String getId() {
    return id;
  }

  public ConnectorSplit getConnectorSplit() {
    return split;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    initializeTaskOnce();
    taskMetrics = new SourceTaskMetrics(getRuntimeContext().getMetricGroup());
  }

  @Override
  public void run(SourceContext<RowData> sourceContext) throws Exception {
    LOG.error("xxx velox plan: {}", Serde.toJson(planNode));
    while (isRunning) {
      UpIterator.State state = task.advance();
      while (state == UpIterator.State.AVAILABLE) {
        RowVector rowVector = task.get();
        List<RowData> rows =
            FlinkRowToVLVectorConvertor.toRowData(rowVector, allocator, outputTypes.get(id));
        for (RowData row : rows) {
          sourceContext.collect(row);
        }
        state = task.advance();
      }

      if (state == UpIterator.State.BLOCKED) {
        LOG.debug("Get empty row");
      } else {
        LOG.info("Velox task finished");
        break;
      }
    }

    task.close();
    session.close();
    memoryManager.close();
    allocator.close();
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO: implement it
    LOG.info("TODO: snapshotState");
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    initializeTaskOnce();
    // TODO: implement it
    LOG.info("TODO: initializeState");
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    LOG.info("TODO: notifyCheckpointComplete");
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    LOG.info("TODO: notifyCheckpointAborted");
  }

  private void initializeTaskOnce() throws Exception {
    if (memoryManager == null) {
      LOG.debug("Running GlutenSourceFunction: " + Serde.toJson(planNode));
      memoryManager = MemoryManager.create(AllocationListener.NOOP);
      session = Velox4j.newSession(memoryManager);
      query =
          new Query(
              planNode, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
      allocator = new RootAllocator(Long.MAX_VALUE);

      task = session.queryOps().execute(query);
      task.addSplit(id, split);
      task.noMoreSplits(id);
    }
  }
}
