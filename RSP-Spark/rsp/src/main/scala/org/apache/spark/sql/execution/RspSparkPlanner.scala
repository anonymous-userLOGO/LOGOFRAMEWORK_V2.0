package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, RspFileSourceStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

/**
 * Created by longhao on 2020/11/27
 */
class RspSparkPlanner(prev: SparkPlanner)
  extends SparkPlanner(prev.sparkContext,prev.conf,prev.experimentalMethods){
  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      PythonEvals ::
        DataSourceV2Strategy ::
        RspFileSourceStrategy ::
        DataSourceStrategy(conf) ::
        SpecialLimits ::
        Aggregation ::
        Window ::
        JoinSelection ::
        InMemoryScans ::
        BasicOperators :: Nil)
}
