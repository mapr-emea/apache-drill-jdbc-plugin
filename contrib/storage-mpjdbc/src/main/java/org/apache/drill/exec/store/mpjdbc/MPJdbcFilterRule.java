/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.mpjdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.adapter.jdbc.JdbcImplementor.Result;
import org.apache.calcite.adapter.jdbc.JdbcQueryProvider;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.util.SqlString;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

public class MPJdbcFilterRule extends StoragePluginOptimizerRule {
  public static final StoragePluginOptimizerRule INSTANCE = new MPJdbcFilterRule();
  public MPJdbcFilterRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
    // TODO Auto-generated constructor stub
  }

  public MPJdbcFilterRule() {
    // TODO Auto-generated constructor stub
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "MPJdbcFilterRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ScanPrel scan = (ScanPrel) call.rel(1);
    FilterPrel filter = (FilterPrel) call.rel(0);
    RexNode condition = filter.getCondition();
    DrillParseContext a = new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner()));
    LogicalExpression conditionExp = DrillOptiq.toDrill(a, scan, condition);
    MPJdbcGroupScan grpScan= (MPJdbcGroupScan) scan.getGroupScan();
    MPJdbcFilterBuilder builder = new MPJdbcFilterBuilder(grpScan, conditionExp);
    MPJdbcScanSpec result = builder.parseTree();

    // If we are not able to push down the complete filter, it needs executing in Drill
    // Therefore we bail before committing the filter to the plan.
    if(builder.isAllExpressionsConverted() == false) {
        return;
    }

    final MPJdbcGroupScan newGroupsScan = new MPJdbcGroupScan(grpScan.getUserName(), grpScan.getStoragePlugin(),
            result, grpScan.getColumns());
    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
    call.transformTo(newScanPrel);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof MPJdbcGroupScan) {
      return super.matches(call);
    }
    return false;
  }

}
