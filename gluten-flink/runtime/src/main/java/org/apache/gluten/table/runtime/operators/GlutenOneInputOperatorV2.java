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
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

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
public class GlutenOneInputOperatorV2<IN, OUT> extends TableStreamOperator<OUT>
    implements OneInputStreamOperator<IN, OUT>, GlutenOperator {

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
  private final Class<IN> inClass;
  private final Class<OUT> outClass;

  public GlutenOneInputOperatorV2(
      PlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass) {
    this.glutenPlan = plan;
    this.id = id;
    this.inputType = inputType;
    this.outputTypes = outputTypes;
    this.inClass = inClass;
    this.outClass = outClass;
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
  public void processElement(StreamRecord<IN> inputData) {
    LOG.info("processElement.");
    LOG.error("xxx gluten plan: {}", Serde.toJson(glutenPlan));
    RowVector inputRowVector = null;
    LOG.error(
        "xxx GlutenOneInputOperatorV2.processElement. inClass: {}, outClass: {}",
        inClass.getName(),
        outClass.getName());
    if (inClass.isAssignableFrom(RowData.class)) {
      GenericRowData rowData = (GenericRowData) inputData.getValue();
      inputRowVector =
          FlinkRowToVLVectorConvertor.fromRowData(rowData, allocator, session, inputType);
    } else if (inClass.isAssignableFrom(RowVector.class)) {
      inputRowVector = (RowVector) inputData.getValue();
      LOG.error(
          "xxx inputData is RowVector directly. rows: {}, rv id: {}",
          inputRowVector.getSize(),
          inputRowVector.id());
    } else {
      throw new UnsupportedOperationException("Unsupported input class: " + inClass.getName());
    }
    inputQueue.put(inputRowVector);
    UpIterator.State state = task.advance();
    while (state == UpIterator.State.AVAILABLE) {
      LOG.info("state == UpIterator.State.AVAILABLE.");
      RowVector outputData = task.get();
      if (outClass.isAssignableFrom(RowVector.class)) {
        LOG.error(
            "xxx collect RowVector directly. rows: {}. rv id: {}",
            outputData.getSize(),
            outputData.id());
        output.collect(new StreamRecord<>((OUT) outputData));
      } else if (outClass.isAssignableFrom(RowData.class)) {
        List<RowData> rows =
            FlinkRowToVLVectorConvertor.toRowData(outputData, allocator, outputTypes.get(id));
        for (RowData row : rows) {
          output.collect(new StreamRecord<>((OUT) row));
        }
      } else {
        throw new UnsupportedOperationException("Unsupported output class: " + outClass.getName());
      }
      state = task.advance();
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
