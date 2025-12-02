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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.config.VeloxQueryConfig;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenOneInputOperatorV2 extends TableStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenOneInputOperatorV2.class);

  private final PlanNode glutenPlan;
  private final String id;
  private final RowType inputType;
  private final Map<String, RowType> outputTypes;

  private MemoryManager memoryManager;
  private Session session;
  private Query query;
  private ExternalStreams.BlockingQueue inputQueue;
  private BufferAllocator allocator;
  private SerialTask task;

  public GlutenOneInputOperatorV2(
      PlanNode plan, String id, RowType inputType, Map<String, RowType> outputTypes) {
    this.glutenPlan = plan;
    this.id = id;
    this.inputType = inputType;
    this.outputTypes = outputTypes;
  }

  @Override
  public void open() throws Exception {
    super.open();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);

    inputQueue = session.externalStreamOps().newBlockingQueue();
    // add a mock input as velox not allow the source is empty.
    PlanNode mockInput =
        new TableScanNode(
            id, inputType, new ExternalStreamTableHandle("connector-external-stream"), List.of());
    glutenPlan.setSources(List.of(mockInput));
    LOG.error("xxx gluten plan: {}", Serde.toJson(glutenPlan));
    query =
        new Query(
            glutenPlan, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
    allocator = new RootAllocator(Long.MAX_VALUE);
    task = session.queryOps().execute(query);
    if (task == null) {
      throw new IllegalStateException(
          "Failed to create velox task for plan: " + Serde.toJson(glutenPlan));
    }
    ExternalStreamConnectorSplit split =
        new ExternalStreamConnectorSplit("connector-external-stream", inputQueue.id());
    task.addSplit(id, split);
    task.noMoreSplits(id);
  }

  @Override
  public void processElement(StreamRecord<RowData> inputData) {
    LOG.info("processElement.");
    GenericRowData rowData = (GenericRowData) inputData.getValue();
    RowVector rowVector = session.rowVectorOps().wrapRowVector(rowData.getLong(0));
    inputQueue.put(rowVector);
    UpIterator.State state = task.advance();
    if (state == UpIterator.State.AVAILABLE) {
      LOG.info("state == UpIterator.State.AVAILABLE.");
      RowVector outputData = task.get();
      // output.collect(new StreamRecord<>(outputData));
      LOG.error("xxx has outputData:");
      Object[] refField = new Object[1];
      refField[0] = Long.valueOf(outputData.id());
      output.collect(new StreamRecord<>(GenericRowData.of(refField)));
    }
  }

  @Override
  public void close() throws Exception {
    LOG.error("xxxx inputQueue {}, task: {}", inputQueue == null, task == null);
    LOG.error("xxx plan: {}", Serde.toJson(glutenPlan));
    inputQueue.close();
    task.close();
    session.close();
    memoryManager.close();
    allocator.close();
  }

  @Override
  public StatefulPlanNode getPlanNode() {
    return null;
  }

  @Override
  public PlanNode getPlanNodeV2() {
    return glutenPlan;
  }

  @Override
  public RowType getInputType() {
    return inputType;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  @Override
  public String getId() {
    return id;
  }
}
