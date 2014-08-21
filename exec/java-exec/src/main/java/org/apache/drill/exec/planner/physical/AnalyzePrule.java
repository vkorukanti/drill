/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import com.google.common.collect.Lists;

import java.util.List;

@SuppressWarnings("unused")
public class AnalyzePrule extends Prule {
  public static final RelOptRule INSTANCE = new AnalyzePrule();

  private static final List<String> FUNCTIONS = ImmutableList.of("statcount", "nonnullstatcount", "ndv", "hll");

  public AnalyzePrule() {
    super(RelOptHelper.some(DrillAnalyzeRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.AnalyzePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAnalyzeRel analyze = (DrillAnalyzeRel) call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);

    SingleRel newAnalyze = new UnpivotMapsPrel(
        new StatsAggPrel(convertedInput, analyze.getCluster(), FUNCTIONS),
        analyze.getCluster(),
        "column",
        FUNCTIONS
    );
    call.transformTo(newAnalyze);
  }
}
