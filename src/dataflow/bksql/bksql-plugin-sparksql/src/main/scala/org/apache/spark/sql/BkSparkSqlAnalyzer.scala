/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.apache.spark.sql

import java.util.concurrent.Callable

import com.tencent.bk.base.dataflow.bksql.deparser.{SparkSqlTableEntity, SparkSqlUdfEntity}
import com.tencent.bk.base.dataflow.bksql.exception.SQLParserException
import com.tencent.bk.base.dataflow.bksql.deparser.SparkSqlUdfEntity
import com.tencent.blueking.bksql.table.ColumnMetadata
import org.apache.spark.sql.BkDataType._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, BkSimpleFunctionRegistry, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.{BkHiveSessionCatalogBuilder, BkHiveUdfCodeGenerator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

object BkSparkSqlAnalyzer {

  def preLoading(): Unit = {
    val tables = new ListBuffer[CatalogTable]()
    tables.append(BkSparkSqlAnalyzer.createTableDesc(
      "test1",
      new StructType()
        .add("thedate", StringType)
        .add("dtEventTime", StringType)
        .add("dtEventTimeStamp", LongType)
        .add("localTime", StringType)
        .add("f_int", IntegerType)
        .add("f_long", LongType)
        .add("f_float", FloatType)
        .add("f_double", DoubleType)
    ))
    val sql =
      """
        |select
        |thedate,
        |count(*) as cnt,
        |sum(f_int) as s1,
        |sum(f_long) as s2
        |from test1
        |group by thedate
      """.stripMargin
    val qe = BkSparkSqlAnalyzer.parserSQL(sql, tables, BkSparkSqlAnalyzer.getFunctionRegistry())
    qe.schema.fields.mkString(", ")
  }

  /**
    *
    * @param sql 查询SQL.
    * @param tables 数据源表.
    * @param functionRegistry 注册的函数信息.
    * @return
    */
  def parserSQL(sql: String,
                tables: Seq[CatalogTable] = Seq.empty,
                functionRegistry: FunctionRegistry,
                hiveUdfFunctions: Seq[CatalogFunction] = Seq.empty): LogicalPlan = {
    try{
      val conf = new SQLConf()
      val sqlParser = new SparkSqlParser(conf)
      val sessionCatalog = createMemorySessionCatalog(conf, functionRegistry)
      // 注册函数(Hive)
      hiveUdfFunctions.foreach(func => sessionCatalog.registerFunction(
        func, overrideIfExists = false))
      // 创建表
      tables.foreach( f => sessionCatalog.createTable(f, true))
      val lPlan = sqlParser.parsePlan(sql)
      val analyzer = createAnalyzer(conf, sessionCatalog)
      val qe = analyzer.executeAndCheck(lPlan)
      qe
    } catch {
      case e: AnalysisException => throw new SQLParserException(e)
      case e: Exception => throw e
    }
  }

  def getFunctionRegistry(udfMap:Map[String, SparkSqlUdfEntity] = Map.empty): FunctionRegistry = {
    val functionRegistry = new BkSimpleFunctionRegistry(udfMap);
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
    functionRegistry
  }

  def createTableDesc(tableName: String, columns: Seq[ColumnMetadata]): CatalogTable = {
    var schema = new StructType();
    columns.foreach( f => {schema = schema.add(f.getColumnName, str2DataType(f.getDataType.name()))})
    createTableDesc(tableName, schema)
  }

  def createTableDesc(tableName: String, schema: StructType): CatalogTable = {
    try {
      val tableDesc = CatalogTable(
        identifier = TableIdentifier(tableName, Some(SessionCatalog.DEFAULT_DATABASE)),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = schema,
        provider = Some("parquet"),
        createTime = 0L,
        createVersion = org.apache.spark.SPARK_VERSION
      )
      tableDesc
    } catch {
      case e: AnalysisException => throw new SQLParserException(e)
      case e: Exception => throw e
    }
  }

  def createHiveCatalogFunctions(udfs: java.util.List[SparkSqlUdfEntity]): Seq[CatalogFunction] = {
    val functions = new ListBuffer[CatalogFunction]()
    udfs.asScala.foreach(entity => {
      functions.append(BkHiveUdfCodeGenerator.generateUdf(
        entity.getFunctionName,
        entity.getInputTypes.toSeq,
        entity.getReturnType,
        entity.getRequiredArgNum)
      )
    })
    functions.toSeq
  }

  def testHiveFunctions(funcName: String, clazz: String): Seq[CatalogFunction] = {
    Seq[CatalogFunction](
      CatalogFunction(FunctionIdentifier(funcName, None),
        clazz,
        Seq[FunctionResource]()))
  }

  lazy val emptyFunctions: Seq[CatalogFunction] = Seq[CatalogFunction]()


  def getOutFields(logicalPlan: LogicalPlan): Seq[SparkSqlTableEntity.Field] = {
    val out = new ListBuffer[SparkSqlTableEntity.Field]()
    try {
      var index = 0;
      logicalPlan.schema.fields.foreach(f => {
        out.append(new SparkSqlTableEntity.Field(index,
          f.name,
          f.name,
          dataType2String(f.dataType, f.name),
          false))
        index += 1
      })
      out.toSeq
    } catch {
      case e: AnalysisException => throw new SQLParserException(e)
      case e: Exception => throw e
    }
  }


  private def createAnalyzer(conf: SQLConf,
                             sessionCatalog: SessionCatalog
                            ): Analyzer = new Analyzer(sessionCatalog, new SQLConf()) {
    //
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new MyFindDataSourceTable(sessionCatalog) +:
       // 不需要支持SQLOnFile
      // new ResolveSQLOnFile(session) +:
        Nil
    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        MyPreProcessTableCreation(conf, sessionCatalog) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        Nil
    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        HiveOnlyCheck +:
        Nil
  }


  private def createMemorySessionCatalog(conf: SQLConf,
                                         functionRegistry: FunctionRegistry) = {

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default",
      CatalogUtils.stringToURI("/tmp/bksql/sparksql/"),
      Map())
    val catalog = new BkInMemoryCatalog()
    catalog.createDatabase(defaultDbDefinition, true)
    // val sessionCatalog = new SessionCatalog(catalog, functionRegistry, conf)
    // 用于创建hive的UDF
    val sessionCatalog = BkHiveSessionCatalogBuilder.createCatalog(catalog, functionRegistry, conf)
    sessionCatalog
  }
}

//////////////////////////////////////////////////////////////////////////////////////////

case class MyBaseRelation(userSpecifiedSchema: StructType)
  extends BaseRelation {

  override def sqlContext: SQLContext = null

  override def sizeInBytes: Long = Long.MaxValue

  override def schema: StructType = userSpecifiedSchema

}

case class MyPreProcessTableCreation(conf1: SQLConf,
                                     catalog1: SessionCatalog) extends Rule[LogicalPlan] {
  // catalog is a def and not a val/lazy val as the latter would introduce a circular reference
  private def catalog = catalog1

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // When we CREATE TABLE without specifying the table schema, we should fail the query if
    // bucketing information is specified, as we can't infer bucketing from data files currently.
    // Since the runtime inferred partition columns could be different from what user specified,
    // we fail the query if the partitioning information is specified.
    case c @ CreateTable(tableDesc, _, None) if tableDesc.schema.isEmpty =>
      if (tableDesc.bucketSpec.isDefined) {
        failAnalysis("Cannot specify bucketing information if the table schema is not specified " +
          "when creating and will be inferred at runtime")
      }
      if (tableDesc.partitionColumnNames.nonEmpty) {
        failAnalysis("It is not allowed to specify partition columns when the table schema is " +
          "not defined. When the table schema is not provided, schema and partition columns " +
          "will be inferred.")
      }
      c

    // When we append data to an existing table, check if the given provider, partition columns,
    // bucket spec, etc. match the existing table, and adjust the columns order of the given query
    // if necessary.
    case c @ CreateTable(tableDesc, SaveMode.Append, Some(query))
      if query.resolved && catalog.tableExists(tableDesc.identifier) =>
      // This is guaranteed by the parser and `DataFrameWriter`
      assert(tableDesc.provider.isDefined)

      val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
      val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
      val tableName = tableIdentWithDB.unquotedString
      val existingTable = catalog.getTableMetadata(tableIdentWithDB)

      if (existingTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException("Saving data into a view is not allowed.")
      }

      // Check if the specified data source match the data source of the existing table.
      val conf = conf1
      val existingProvider = DataSource.lookupDataSource(existingTable.provider.get, conf)
      val specifiedProvider = DataSource.lookupDataSource(tableDesc.provider.get, conf)
      // TODO: Check that options from the resolved relation match the relation that we are
      // inserting into (i.e. using the same compression).
      if (existingProvider != specifiedProvider) {
        throw new AnalysisException(s"The format of the existing table $tableName is " +
          s"`${existingProvider.getSimpleName}`. It doesn't match the specified format " +
          s"`${specifiedProvider.getSimpleName}`.")
      }
      tableDesc.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          throw new AnalysisException(
            s"The location of the existing table ${tableIdentWithDB.quotedString} is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${tableDesc.location}`.")
        case _ =>
      }

      if (query.schema.length != existingTable.schema.length) {
        throw new AnalysisException(
          s"The column number of the existing table $tableName" +
            s"(${existingTable.schema.catalogString}) doesn't match the data schema" +
            s"(${query.schema.catalogString})")
      }

      val resolver = conf1.resolver
      val tableCols = existingTable.schema.map(_.name)

      // As we are inserting into an existing table, we should respect the existing schema and
      // adjust the column order of the given dataframe according to it, or throw exception
      // if the column names do not match.
      val adjustedColumns = tableCols.map { col =>
        query.resolve(Seq(col), resolver).getOrElse {
          val inputColumns = query.schema.map(_.name).mkString(", ")
          throw new AnalysisException(
            s"cannot resolve '$col' given input columns: [$inputColumns]")
        }
      }

      // Check if the specified partition columns match the existing table.
      val specifiedPartCols = CatalogUtils.normalizePartCols(
        tableName, tableCols, tableDesc.partitionColumnNames, resolver)
      if (specifiedPartCols != existingTable.partitionColumnNames) {
        val existingPartCols = existingTable.partitionColumnNames.mkString(", ")
        throw new AnalysisException(
          s"""
             |Specified partitioning does not match that of the existing table $tableName.
             |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
             |Existing partition columns: [$existingPartCols]
          """.stripMargin)
      }

      // Check if the specified bucketing match the existing table.
      val specifiedBucketSpec = tableDesc.bucketSpec.map { bucketSpec =>
        CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
      }
      if (specifiedBucketSpec != existingTable.bucketSpec) {
        val specifiedBucketString =
          specifiedBucketSpec.map(_.toString).getOrElse("not bucketed")
        val existingBucketString =
          existingTable.bucketSpec.map(_.toString).getOrElse("not bucketed")
        throw new AnalysisException(
          s"""
             |Specified bucketing does not match that of the existing table $tableName.
             |Specified bucketing: $specifiedBucketString
             |Existing bucketing: $existingBucketString
          """.stripMargin)
      }

      val newQuery = if (adjustedColumns != query.output) {
        Project(adjustedColumns, query)
      } else {
        query
      }

      c.copy(
        tableDesc = existingTable,
        query = Some(DDLPreprocessingUtils.castAndRenameQueryOutput(
          newQuery, existingTable.schema.toAttributes, conf)))

    // Here we normalize partition, bucket and sort column names, w.r.t. the case sensitivity
    // config, and do various checks:
    //   * column names in table definition can't be duplicated.
    //   * partition, bucket and sort column names must exist in table definition.
    //   * partition, bucket and sort column names can't be duplicated.
    //   * can't use all table columns as partition columns.
    //   * partition columns' type must be AtomicType.
    //   * sort columns' type must be orderable.
    //   * reorder table schema or output of query plan, to put partition columns at the end.
    case c @ CreateTable(tableDesc, _, query) if query.forall(_.resolved) =>
      if (query.isDefined) {
        assert(tableDesc.schema.isEmpty,
          "Schema may not be specified in a Create Table As Select (CTAS) statement")

        val analyzedQuery = query.get
        val normalizedTable = normalizeCatalogTable(analyzedQuery.schema, tableDesc)

        val output = analyzedQuery.output
        val partitionAttrs = normalizedTable.partitionColumnNames.map { partCol =>
          output.find(_.name == partCol).get
        }
        val newOutput = output.filterNot(partitionAttrs.contains) ++ partitionAttrs
        val reorderedQuery = if (newOutput == output) {
          analyzedQuery
        } else {
          Project(newOutput, analyzedQuery)
        }

        c.copy(tableDesc = normalizedTable, query = Some(reorderedQuery))
      } else {
        val normalizedTable = normalizeCatalogTable(tableDesc.schema, tableDesc)

        val partitionSchema = normalizedTable.partitionColumnNames.map { partCol =>
          normalizedTable.schema.find(_.name == partCol).get
        }

        val reorderedSchema =
          StructType(normalizedTable.schema.filterNot(partitionSchema.contains) ++ partitionSchema)

        c.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
      }
  }

  private def normalizeCatalogTable(schema: StructType, table: CatalogTable): CatalogTable = {
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema,
      "in the table definition of " + table.identifier,
      conf1.caseSensitiveAnalysis)

    val normalizedPartCols = normalizePartitionColumns(schema, table)
    val normalizedBucketSpec = normalizeBucketSpec(schema, table)

    normalizedBucketSpec.foreach { spec =>
      for (bucketCol <- spec.bucketColumnNames if normalizedPartCols.contains(bucketCol)) {
        throw new AnalysisException(s"bucketing column '$bucketCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
      for (sortCol <- spec.sortColumnNames if normalizedPartCols.contains(sortCol)) {
        throw new AnalysisException(s"bucket sorting column '$sortCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
    }

    table.copy(partitionColumnNames = normalizedPartCols, bucketSpec = normalizedBucketSpec)
  }

  private def normalizePartitionColumns(schema: StructType, table: CatalogTable): Seq[String] = {
    val normalizedPartitionCols = CatalogUtils.normalizePartCols(
      tableName = table.identifier.unquotedString,
      tableCols = schema.map(_.name),
      partCols = table.partitionColumnNames,
      resolver = conf1.resolver)

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartitionCols,
      "in the partition schema",
      conf1.resolver)

    if (schema.nonEmpty && normalizedPartitionCols.length == schema.length) {
      if (DDLUtils.isHiveTable(table)) {
        // When we hit this branch, it means users didn't specify schema for the table to be
        // created, as we always include partition columns in table schema for hive serde tables.
        // The real schema will be inferred at hive metastore by hive serde, plus the given
        // partition columns, so we should not fail the analysis here.
      } else {
        failAnalysis("Cannot use all columns for partition columns")
      }

    }

    schema.filter(f => normalizedPartitionCols.contains(f.name)).map(_.dataType).foreach {
      case _: AtomicType => // OK
      case other => failAnalysis(s"Cannot use ${other.simpleString} for partition column")
    }

    normalizedPartitionCols
  }

  private def normalizeBucketSpec(schema: StructType, table: CatalogTable): Option[BucketSpec] = {
    table.bucketSpec match {
      case Some(bucketSpec) =>
        val normalizedBucketSpec = CatalogUtils.normalizeBucketSpec(
          tableName = table.identifier.unquotedString,
          tableCols = schema.map(_.name),
          bucketSpec = bucketSpec,
          resolver = conf1.resolver)

        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.bucketColumnNames,
          "in the bucket definition",
          conf1.resolver)
        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.sortColumnNames,
          "in the sort definition",
          conf1.resolver)

        normalizedBucketSpec.sortColumnNames.map(schema(_)).map(_.dataType).foreach {
          case dt if RowOrdering.isOrderable(dt) => // OK
          case other => failAnalysis(s"Cannot use ${other.simpleString} for sorting column")
        }

        Some(normalizedBucketSpec)

      case None => None
    }
  }

  private def failAnalysis(msg: String) = throw new AnalysisException(msg)
}



class MyFindDataSourceTable(catalog1: SessionCatalog) extends Rule[LogicalPlan] {
  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = catalog1
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        LogicalRelation(new MyBaseRelation(table.schema), table)
      }
    })
  }

  private def readHiveTable(table: CatalogTable): LogicalPlan = {
    HiveTableRelation(
      table,
      // Hive table columns are always nullable.
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
      if DDLUtils.isDatasourceTable(tableMeta) =>
      i.copy(table = readDataSourceTable(tableMeta))

    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _) =>
      i.copy(table = readHiveTable(tableMeta))

    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      readDataSourceTable(tableMeta)

    case UnresolvedCatalogRelation(tableMeta) =>
      readHiveTable(tableMeta)
  }
}


