package com.axpo.ventus.sparklib.fcmetered

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

import org.json.JSONObject;
import org.json.JSONArray;

import org.codehaus.jackson.JsonNode

import com.axpo.ventus.sparklib.utils.SparkUtils
import com.axpo.ventus.sparklib.utils.LogUtils
import com.axpo.ventus.sparklib.utils.ExceptionUtils

import com.axpo.ventus.javaclient.RestClient
import com.axpo.ventus.javaclient.TimeOutConfig

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode

import com.minsait.onesait.platform.comms.protocol.enums.SSAPQueryType

import java.text.SimpleDateFormat
import java.util.Date
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import com.axpo.ventus.sparklib.utils.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import java.util.Properties
import com.axpo.ventus.sparklib.utils.ResourceUtils

object FCMeteredLDProcess {

  var debug = false
  val APP_NAME = "FCMeteredProcessLD"
  val MODIFIED = "modified"
  val OFFSET_IN_DAYS = 30
  val sdfBDB: SimpleDateFormat = new SimpleDateFormat(DateUtils.bdb_date_format)
  val sdfRTDB: SimpleDateFormat = new SimpleDateFormat(DateUtils.rtdb_date_format)
  var props = new Properties()

  /*Ontologies*/
  val ONTOLOGY_MP = "CORE_METER_POINT"
  val ONTOLOGY_MPD = "CORE_METER_POINT_DETAIL"
  val ONTOLOGY_FCG = "CORE_FORECAST_GROUP"
  val ONTOLOGY_EV_AV = "CORE_EV_PLANNED_AVAILABILITY"
  val ONTOLOGY_TS_DEF = "CORE_TIMESERIES_DEFINITION"

  val ONTOLOGY_TS = "CORE_TS_RAW_DATA"
  val ONTOLOGY_AV = "CORE_TS_PLANNED_AVAILABILITY"

  val ONTOLOGY_FCM = "AGG_FC_METERED"
  val ONTOLOGY_FCM_ = "AGG_FC_METERED_LD"
  val ONTOLOGY_BAV = "AGG_TS_BAV"
  val ONTOLOGY_BAF = "AGG_TS_BAF"

  val ONTOLOGY_FC4FCM = "AGG_FC4FCM"

  /*Values*/
  val ID_CAPACITY = 16
  val ID_GRID_LIMIT = 10
  val ID_GROUP_SINGLE = 2
  val ID_GROUP_AGG = 1
  val ID_P50 = 3
  val ID_P50_ADJ = 6
  val ID_MT_EVENT = 11

  /*Schemas*/
  val schema_MPSTA = StructType(List(
    StructField("idMP", IntegerType, true)))
  
  val schema_MPD = StructType(List(
    StructField("idMP", IntegerType, true),
    StructField("startValidity", (new StructType).add("$date", StringType), true),
    StructField("endValidity", (new StructType).add("$date", StringType), true)))

  val schema_MP_DET = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("country", StringType, true),
    StructField("client", StringType, true),
    StructField("class", StringType, true)))

  val schema_EV_AV = StructType(List(
    StructField("idtimeseriesdefinition", IntegerType, true),
    StructField("startvalidity", StringType, true),
    StructField("endvalidity", StringType, true),
    StructField("value", DoubleType, true),
    StructField("startReferenceValidity", StringType, true),
    StructField("endreferencevalidity", StringType, true),
    StructField("created", StringType, true),
    StructField("modified", StringType, true)))

  val schema_TS_DEF = StructType(List(
    StructField("idmeterpoint", IntegerType, true),
    StructField("id", IntegerType, true),
    StructField("idmetertype", IntegerType, true)))

 val schema_AV = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("idMP", IntegerType, true),
    StructField("ts", TimestampType, true),
    StructField("hour", IntegerType, true),
    StructField("day", TimestampType, true),   
    StructField("offset", IntegerType, true),
    StructField("latFG", IntegerType, true),
    StructField("mFilDA1", TimestampType, true),
    StructField("mFilDA2", TimestampType, true),
    StructField("mFilI61", TimestampType, true),
    StructField("mFilI62", TimestampType, true),
    StructField("mFilI81", TimestampType, true),
    StructField("mFilI82", TimestampType, true),
    StructField("mFilI121", TimestampType, true),
    StructField("mFilI122", TimestampType, true),
    StructField("mFilI161", TimestampType, true),
    StructField("mFilI162", TimestampType, true),
    StructField("mFilI181", TimestampType, true),
    StructField("mFilI182", TimestampType, true)))
  
  val schema_AVACT = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("ts", TimestampType, true),
    StructField("value", DoubleType, true)))    

  val schema_CAP = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("idMP", IntegerType, true),
    StructField("idMT", IntegerType, true),
    StructField("ts", TimestampType, true),
    StructField("values", DoubleType, true)))    

  val schema_GL = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("value", DoubleType, true),
    StructField("ts", TimestampType, true)))
 
   val schema_BAV = StructType(List(
    StructField("idFG", IntegerType, true),
    StructField("idMP", IntegerType, true),
    StructField("ts", TimestampType, true),
    StructField("v", DoubleType, true)))
    
  /*Queries SQL*/
  val QUERY_METERED_MP = "idStatus in (1,2,4)"
  val QUERY_MP_DETAILS = "idStatus in (1,2,4)"
  val QUERY_TS_DEF = "idmetertype = " + ID_MT_EVENT + ""
  val QUERY_FC = "ts_h >= \"<dtFrom>\" and ts_h <= \"<dtTo>\""
  
  /*Queries NoSQL*/
  val QUERY_METERED_BAV =
    "[{$match:{$and:[{\"data.lastM\":{$gte:ISODate('<dtlastMod>')}},{\"data.idMP\":{$gte:0}},]}}," +
      "{$project:{ \"_id\":0, \"idFG\":\"$data.idFG\", \"idMP\":\"$data.idMP\" , \"ts\": \"$data.ts\",\"values\": \"$data.values.v\"}}," +
      "{$unwind: {path:\"$values\", includeArrayIndex:\"hours\"}}," +
      "{$project:{ \"_id\":0, \"idFG\":\"$idFG\",\"idMP\":\"$idMP\", \"ts\": \"$ts\", \"hours\": \"$hours\", \"values\": \"$values.v\"}}," +
      "{$unwind: {path:\"$values\", includeArrayIndex:\"minutes\"}}," +
      "{$project:{ \"_id\":0, \"idFG\":\"$idFG\",\"idMP\":\"$idMP\", \"ts\":{$add:[\"$ts\",{$multiply:[\"$hours\",60,60,1000]},{$multiply:[\"$minutes\",60,60,1000]}]}, \"v\": \"$values.v.v\", \"mod\": \"$values.m\" }}," +
      "{$match:{\"v\":{$ne:null}}}," +
      "{$project:{\"idMP\":\"$idMP\", \"idFG\":\"$idFG\", \"ts\":\"$ts\" , \"v\": \"$v\"}}]"
 
  val QUERY_FORECAST_V_CAP =
    "[{$match:{$and:[{\"data.idMT\":" + ID_CAPACITY + "},{\"data.ts\":{$gte:ISODate('<dtFromFC>')}}, {\"data.ts\":{$lte:ISODate('<dtToFC>')}},{\"data.idMP\":{$gte:0}}]}}," +
      "{$project: {\"idFG\": \"$data.idFG\",\"idMP\": \"$data.idMP\",\"idMT\": \"$data.idMT\",\"gr\": \"$data.gr\",\"ts\": \"$data.ts\",\"values\": \"$data.values.v\"}}," +
      "{$unwind: {path:\"$values\", includeArrayIndex:\"hours\"}}," +
      "{$project:{\"idFG\":\"$idFG\",\"idMP\": \"$idMP\",\"idMT\": \"$idMT\",\"ts\":\"$ts\",\"gr\": \"$gr\",\"hours\": \"$hours\",\"values\": \"$values.v\"}}," +
      "{$unwind: {path:\"$values\", includeArrayIndex:\"minutes\"}}," +
      "{$project:{\"idFG\":\"$idFG\",\"idMP\": \"$idMP\",\"idMT\": \"$idMT\",\"gr\": \"$gr\",\"hours\": \"$hours\",\"ts_hourly\":{$add:[\"$ts\",{$multiply:[\"$hours\",60,60,1000]}]},\"ts\":{$add:[\"$ts\",{$multiply:[\"$hours\",60,60,1000]},{$multiply:[\"$minutes\",\"$gr\",60,1000]}]},\"values\": \"$values.v\"}}," +
      "{$match:{\"ts\":{$ne:null}}}," +
      "{$project:{\"idFG\":\"$idFG\",\"idMP\": \"$idMP\",\"idMT\": \"$idMT\",\"ts\":\"$ts\",\"ts_hourly\":\"$ts_hourly\",\"gr\": \"$gr\",\"values\": \"$values\"}}," +
      "{$group:{_id:{idFG: \"$idFG\",idMP: \"$idMP\",idMT: \"$idMT\", ts :\"$ts_hourly\"}, values: {$max: \"$values\"} }}," +
      "{$project:{\"_id\": 0,\"idFG\":\"$_id.idFG\",\"idMP\": \"$_id.idMP\",\"idMT\": \"$_id.idMT\",\"ts\":\"$_id.ts\",\"values\": \"$values\"}}]"

      
 val QUERY_AVAILABILITY_V_AT =
    "[{$match:{$and:[{\"data.ts\":{$gte:ISODate('<dtFrom>')}},{\"data.ts\":{$lte:ISODate('<dtTo>')}},{\"data.idMT\": 9 },{\"data.idMP\":{$gte:0}}]}}," +
      "{$project:{idFG:\"$data.idFG\",idMP: \"$data.idMP\",gr:\"$data.gr\", latFG:\"$data.latFG\",offset:\"$data.offset\",value:\"$data.values\",ts:\"$data.ts\",day:\"$data.ts\"}}," +
      "{$unwind:{path:\"$value.v\",includeArrayIndex:\"hour\"}}," +
      "{$project:{idFG:\"$idFG\",idMP:\"$idMP\",gr:\"$gr\",latFG:\"$latFG\",offset:\"$offset\",ts:\"$ts\",hour:\"$hour\",day:\"$day\"}}," +
      "{$project:{idFG:\"$idFG\",idMP:\"$idMP\",gr:\"$gr\",latFG:\"$latFG\",offset:\"$offset\",day:\"$day\" " +
      ",ts:{$add:[\"$ts\",{$multiply:[\"$hour\",60,60,1000]},{$multiply:[\"$gr\",60,1000]}]},ts_h:{$add:[\"$ts\",{$multiply:[\"$hour\",60,60,1000]}]}}}," +
      "{$project:{idFG:\"$idFG\",idMP:\"$idMP\",latFG:\"$latFG\",offset:\"$offset\",day:\"$day\",ts:\"$ts_h\"," +
      "mFilDA1:{$subtract:[{$add:[\"$day\",{$multiply:[1000,60,60,{$multiply:[-24,1]}]},\"$latFG\"]},\"$offset\"]}," +
      "mFilDA2:{$subtract:[{$add:[\"$day\",{$multiply:[1000,60,60,{$multiply:[-24,0]}]},\"$latFG\"]},\"$offset\"]}," +
      "mFilI61:{$subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[6,0]}]}]},\"$offset\"]}," +
      "mFilI62: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[6,24]}]}]},\"$offset\"]}," +
      "mFilI81: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[8,0]}]}]},\"$offset\"]}," +
      "mFilI82: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[8,24]}]}]},\"$offset\"]}," +
      "mFilI121: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[12,0]}]}]},\"$offset\"]}," +
      "mFilI122: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[12,24]}]}]},\"$offset\"]}," +
      "mFilI161: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[16,0]}]}]},\"$offset\"]}," +
      "mFilI162: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[16,24]}]}]},\"$offset\"]}," +
      "mFilI181: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[18,0]}]}]},\"$offset\"]}," +
      "mFilI182: { $subtract:[{$add:[\"$day\",{$multiply: [1000,60,60,{$add:[18,24]}]}]},\"$offset\"]}," +
      "hour: {$hour:\"$ts\"}}}" +
      "]"

      
  val QUERY_AVAILABILITY_V_ACT =
    "[{$match:{$and:[{\"data.idMT\":9},{\"data.ts\":{$gte:ISODate('<dtFrom>')}},{\"data.ts\":{$lte:ISODate('<dtTo>')}}]}}," +
      "{$project:{_id:0,idFG: \"$data.idFG\",idMP:\"$data.idMP\",ts:\"$data.ts\",values:\"$data.values\",gr:\"$data.gr\"}}," +
      "{$unwind:{path:\"$values.v\", includeArrayIndex:\"hours\"}}," +
      "{$project:{idFG:\"$idFG\",idMP:\"$idMP\",ts:\"$ts\",hours:\"$hours\",values:\"$values.v\",gr:\"$gr\"}}," +
      "{$unwind:{path:\"$values.v\", includeArrayIndex:\"minutes\"}}," +
      "{$project:{idFG:\"$idFG\",idMP:\"$idMP\"," +
      "ts:{$add:[\"$ts\",{$multiply:[\"$hours\",60,60,1000]},{$multiply:[\"$minutes\",\"$gr\",60,1000]}]}," +
      "ts_h:{$add:[\"$ts\",{$multiply:[\"$hours\",60,60,1000]}]}," +
      "value:\"$values.v.v\"}}," +
      "{$match:{value:{$ne:null}}}," +
      "{$group:{_id:{idFG:\"$idFG\",ts:\"$ts_h\"},value: {$avg:\"$value\"}}}," +
      "{$project:{idFG:\"$_id.idFG\",ts:\"$_id.ts\",value:\"$value\"}}" +
      "]"

  val QUERY_GRID_LIMIT =
    "[{$match:{$and:[{\"data.ts\":{$gte:ISODate('<dtFromFC>')}}, {\"data.ts\":{$lte:ISODate('<dtToFC>')}},{\"data.idMT\":" + ID_GRID_LIMIT + "},{\"data.idMP\":{$gte:0}}]}}," +
      "{$project:{idFG:\"$data.idFG\",idMP: \"$data.idMP\",value:\"$data.values\",ts:\"$data.ts\"}}," +
      "{$unwind:{path:\"$value.v\",includeArrayIndex: \"hour\"}}," +
      "{$project:{idFG:\"$idFG\",idMP: \"$idMP\",ts:\"$ts\",hour:\"$hour\",minSize:{\"$size\":\"$value.v.v\"},value:\"$value.v.v\"}}," +
      "{$unwind:{path:\"$value\",includeArrayIndex: \"xMinutal\"}}," +
      "{$project:{idFG:\"$idFG\",idMP: \"$idMP\",hour:\"$hour\",xMinutal:\"$xMinutal\",value:\"$value.v\",ts:\"$ts\",modified: \"$value.m\",min: {$multiply:[\"$xMinutal\",{$divide: [60, \"$minSize\"]}]}}}," +
      "{$project:" +
      "{idFG:\"$idFG\",idMP: \"$idMP\",value:\"$value\",modified: \"$modified\",ts:{$add:[\"$ts\",{$multiply:[\"$hour\",60,60,1000]},{$multiply:[\"$min\",60,1000]}]},ts_h:{$add:[\"$ts\",{$multiply:[\"$hour\",60,60,1000]}]}}}," +
      "{\"$group\":{ _id: {idFG:\"$idFG\",ts: \"$ts_h\",},value:{$sum:\"$value\"},modified:{$max:\"$modified\"}}}," +
      "{$project:{_id: 0,idFG:\"$_id.idFG\",value:\"$value\",ts:\"$_id.ts\"}}]"

  /*def getQueries*/
  def getQueryQUERY_METERED_BAV(dtlastMod: String): String = {
    QUERY_METERED_BAV.replaceAll("<dtlastMod>", dtlastMod)
  }

  def getQueryQUERY_FORECAST_V_CAP(dtFromFC: String, dtToFC: String): String = {
    QUERY_FORECAST_V_CAP.replaceAll("<dtFromFC>", dtFromFC)
      .replaceAll("<dtToFC>", dtToFC)
  }

  def getQueryQUERY_AVAILABILITY_V_AT(dtFromFC: String, dtToFC: String): String = {
    QUERY_AVAILABILITY_V_AT.replaceAll("<dtFrom>", dtFromFC)
      .replaceAll("<dtTo>", dtToFC)
  }

  def getQueryQUERY_AVAILABILITY_V_ACT(dtFrom: String, dtTo: String): String = {
    QUERY_AVAILABILITY_V_ACT.replaceAll("<dtFrom>", dtFrom)
      .replaceAll("<dtTo>", dtTo)
  }

  def getQueryQUERY_GRID_LIMIT(dtFrom: String, dtTo: String): String = {
    QUERY_GRID_LIMIT.replaceAll("<dtFromFC>", dtFrom)
      .replaceAll("<dtToFC>", dtTo)
  }

  def getQueryQUERY_FC(dtFrom: String, dtTo: String): String = {
    QUERY_FC.replaceAll("<dtFrom>", dtFrom)
      .replaceAll("<dtTo>", dtTo)
  }
  
  def main(args: Array[String]): Unit = {

    val idProcess = args(0)
    val env = args(1)
    val master = args(2)
    val deployMode = args(3)
    val dtFromMet = args(4)
    val dtToMet = args(5)
    val dtFromFC = args(6)
    val dtToFC = args(7)
    val dtFromFCV = args(8)
    val dtToFCV = args(9)
    val dtTimeTd = args(10)
    val dtlastMod = args(11)
    if (args.length > 12 && (args(12).equalsIgnoreCase("true") || args(12).equals("1"))) {
      debug = true
    }
    props = ResourceUtils.loadProperties(env)
    execute(idProcess, master, deployMode, dtFromMet, dtToMet, dtFromFC, dtToFC, dtFromFCV, dtToFCV, dtTimeTd, dtlastMod)

  }

  def execute(idProcess: String, master: String, deployMode: String, dtFromMet: String, dtToMet: String, dtFromFC: String, dtToFC: String, dtFromFCV: String, dtToFCV: String, dtTimeTd: String, dtlastMod: String): Unit = {

    val restClient: RestClient = SparkUtils.createRestClient(props.getProperty("onesaitplatform.iotclient.urlExtRestIoTBroker"))
    val sessionKey: String = restClient.connect(
      props.getProperty("onesaitplatform.iotclient.token"),
      props.getProperty("onesaitplatform.iotclient.device"),
      APP_NAME,
      true)
    val sparkSession: SparkSession = SparkUtils.initSparkSession(master, APP_NAME, deployMode)
    val startDate = DateUtils.now()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val idLog = restClient.insert(LogUtils.ONTOLOGY_LOG, LogUtils.logRunning(idProcess, startDate))
    try {
      calcFCMetered(restClient, sparkSession, dtFromMet, dtToMet, dtFromFC, dtToFC, dtFromFCV, dtToFCV, dtTimeTd, dtlastMod)
      val endDate = DateUtils.now()
      restClient.update(LogUtils.ONTOLOGY_LOG, LogUtils.logOK(idProcess, startDate, endDate), idLog)
    } catch {
      case ex: Exception =>
        println(ex.getMessage)
        val endDate = DateUtils.now()
        restClient.update(LogUtils.ONTOLOGY_LOG, LogUtils.logNoOK(idProcess, startDate, endDate), idLog);
        restClient.insert(ExceptionUtils.ONTOLOGY_ERR, ExceptionUtils.logError(idProcess, ex.getMessage()));
        throw new Exception(ex);
    } finally {
      sparkSession.close()
      restClient.disconnect()
    }
  }

  def getDFAV_DA(df: DataFrame, lowInterval: Integer, highInterval: Integer): DataFrame = {

    var dfTmp = df.filter(df("difm") >= 0).withColumn("row_number", row_number().over(Window.partitionBy("idFG", "idMP", "ts").orderBy(asc("difm"))))
    dfTmp = dfTmp.filter(col("row_number") === 1)
    dfTmp = dfTmp.withColumn("offset", col("offset") / 3600000)
    dfTmp = dfTmp.groupBy("idFG", "ts").agg(avg("value"), max("offset"), max("hour"))
      .withColumnRenamed("avg(value)", "value")
      .withColumnRenamed("max(offset)", "offset")
      .withColumnRenamed("max(hour)", "hour")
    dfTmp = dfTmp.withColumn("diff_type", col("hour") + col("offset"))
    dfTmp = dfTmp.filter(col("diff_type") >= lowInterval)
    dfTmp = dfTmp.filter(col("diff_type") <= highInterval)
    dfTmp = dfTmp.drop("diff_type")
    dfTmp
  }

  def calcFCMetered(restClient: RestClient, sparkSession: SparkSession, dtFromMet: String, dtToMet: String, dtFromFC: String, dtToFC: String, dtFromFCV: String, dtToFCV: String, dtTimeTdX: String, dtlastMod: String): Unit = {

    val DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    val sdf = new SimpleDateFormat(DATE_FORMAT);
    val dtFrom: Date = sdf.parse(dtFromMet)
    val dtTo: Date = sdf.parse(dtToMet)

    val DATE_FORMAT2 = "yyyy-MM-dd' 'HH:mm:ss"
    val sdf2 = new SimpleDateFormat(DATE_FORMAT2);
    val odtFrom = sdf2.format(dtFrom)
    val odtTo = sdf2.format(dtTo)    
    
    println("Start forecast " + new Date())

    var query = getQueryQUERY_FC(odtFrom, odtTo)
    var dfFC = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_FC4FCM, query)
    if (debug) println(dfFC.count())

    println("end  forecast " + new Date())
    
    println("Start availabilities " + new Date())

    var df_EV_AV = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_EV_AV)
    var df_TS_DEF = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_TS_DEF, QUERY_TS_DEF)

    df_EV_AV = df_EV_AV.select("idtimeseriesdefinition","startvalidity","endvalidity","value","startReferenceValidity","endreferencevalidity","created","modified")
    df_TS_DEF = df_TS_DEF.select("idmeterpoint","id","idmetertype").withColumnRenamed("id", "idtimeseriesdefinition")

    var df_EV_AVT = df_EV_AV.join(df_TS_DEF, Seq("idtimeseriesdefinition"), "inner")

    var df_EV_AV_REF = df_EV_AV.groupBy("idTimeSeriesDefinition", "startReferenceValidity", "endReferenceValidity")
                               .agg(min("startValidity").as("minStartValidity"), max("endValidity").as("maxEndValidity"))

    df_EV_AVT = df_EV_AVT.join(df_EV_AV_REF, Seq("idtimeseriesdefinition", "startReferenceValidity" , "endReferenceValidity"), "left")
 
    df_EV_AVT = df_EV_AVT.select("idmeterpoint", "startvalidity", "endvalidity", "value", "startReferenceValidity", "endreferencevalidity", "minStartValidity", "maxEndValidity", "modified")

        query = getQueryQUERY_AVAILABILITY_V_AT(dtFromFC, dtToFC)
    var dfAV = SparkUtils.getDF(sparkSession, props.getProperty("rtdb.url"),ONTOLOGY_AV, query, schema_AV)
        
    var dfTmp1 = dfAV
      .join(df_EV_AVT, dfAV("idMP") === df_EV_AVT("idmeterpoint")
        && dfAV("ts") >= df_EV_AVT("startreferencevalidity")
        && dfAV("ts") < df_EV_AVT("minStartvalidity"), "left_outer")
    var dfTmp2 = dfAV
      .join(df_EV_AVT, dfAV("idMP") === df_EV_AVT("idmeterpoint")
        && dfAV("ts") > df_EV_AVT("maxEndValidity")
        && dfAV("ts") <= df_EV_AVT("endreferencevalidity"), "left_outer")
    var dfTmp3 = dfAV
      .join(df_EV_AVT, dfAV("idMP") === df_EV_AVT("idmeterpoint")
        && dfAV("ts") >= df_EV_AVT("startvalidity")
        && dfAV("ts") <= df_EV_AVT("endvalidity"), "left_outer")
    dfAV = dfTmp1
      .union(dfTmp2)
      .union(dfTmp3)
    dfAV = dfAV
      .drop("idmeterpoint", "startvalidity", "endvalidity", "startReferenceValidity", "endreferencevalidity", "minStartValidity", "maxEndValidity")

    var dfAVAP_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilDA1").cast(LongType) - col("modified").cast(LongType)), 0, 23)  
    var dfAVAP_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilDA2").cast(LongType) - col("modified").cast(LongType)), 24, 48)
    var dfAVINT6_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI61").cast(LongType) - col("modified").cast(LongType)), 0, 23)
    var dfAVINT6_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI62").cast(LongType) - col("modified").cast(LongType)), 24, 48)
    var dfAVINT8_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI81").cast(LongType) - col("modified").cast(LongType)), 0, 23)
    var dfAVINT8_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI82").cast(LongType) - col("modified").cast(LongType)), 24, 48)
    var dfAVINT12_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI121").cast(LongType) - col("modified").cast(LongType)), 0, 23)
    var dfAVINT12_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI122").cast(LongType) - col("modified").cast(LongType)), 24, 48)
    var dfAVINT16_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI161").cast(LongType) - col("modified").cast(LongType)), 0, 23)
    var dfAVINT16_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI162").cast(LongType) - col("modified").cast(LongType)), 24, 48)
    var dfAVINT18_1 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI181").cast(LongType) - col("modified").cast(LongType)), 0, 23)
    var dfAVINT18_2 = getDFAV_DA(dfAV.withColumn("difm", col("mFilI182").cast(LongType) - col("modified").cast(LongType)), 24, 48)

    query = getQueryQUERY_AVAILABILITY_V_ACT(dtFromFC, dtToFC)
    var dfAVACT = SparkUtils.getDF(sparkSession, props.getProperty("rtdb.url"), ONTOLOGY_AV, query,schema_AVACT)
  
    var dfAVAP = dfAVAP_1
      .union(dfAVAP_2)
    var dfAVINT6 = dfAVINT6_1.drop("hour", "offset")
      .union(dfAVINT6_2.drop("hour", "offset"))
    var dfAVINT8 = dfAVINT8_1.drop("hour", "offset")
      .union(dfAVINT8_2.drop("hour", "offset"))
    var dfAVINT12 = dfAVINT12_1.drop("hour", "offset")
      .union(dfAVINT12_2.drop("hour", "offset"))
    var dfAVINT16 = dfAVINT16_1.drop("hour", "offset")
      .union(dfAVINT16_2.drop("hour", "offset"))
    var dfAVINT18 = dfAVINT18_1.drop("hour", "offset")
      .union(dfAVINT18_2.drop("hour", "offset"))

    var dfAVT = dfAVAP
      .withColumnRenamed("value", "v_AVT")
      .join(dfAVINT6.withColumnRenamed("value", "v_IV6"), Seq("idFG", "ts"), "left_outer")
      .join(dfAVINT8.withColumnRenamed("value", "v_IV8"), Seq("idFG", "ts"), "left_outer")
      .join(dfAVINT12.withColumnRenamed("value", "v_IV12"), Seq("idFG", "ts"), "left_outer")
      .join(dfAVINT16.withColumnRenamed("value", "v_IV16"), Seq("idFG", "ts"), "left_outer")
      .join(dfAVINT18.withColumnRenamed("value", "v_IV18"), Seq("idFG", "ts"), "left_outer")
      .join(dfAVACT.withColumnRenamed("value", "v_AV"), Seq("idFG", "ts"), "left_outer")

    dfAVT
      .withColumn("offset", col("offset").cast(IntegerType))
    dfAVT = dfAVT
      .withColumn("v_IintV2", when(col("hour") >= (lit(0) - col("offset")) && col("hour") < (lit(12) - col("offset")), col("v_AVT"))
        .when(col("hour") >= (lit(12) - col("offset")) && col("hour") < (lit(24) - col("offset")), col("v_IV12"))
        .otherwise(col("v_AVT")))
      .withColumn("v_IintV3", when(col("hour") >= (lit(0) - col("offset")) && col("hour") < (lit(8) - col("offset")), col("v_AVT"))
        .when(col("hour") >= (lit(8) - col("offset")) && col("hour") < (lit(16) - col("offset")), col("v_IV8"))
        .when(col("hour") >= (lit(16) - col("offset")) && col("hour") < (lit(24) - col("offset")), col("v_IV16"))
        .otherwise(col("v_AVT")))
      .withColumn("v_IintV4", when(col("hour") >= (lit(0) - col("offset")) && col("hour") < (lit(6) - col("offset")), col("v_AVT"))
        .when(col("hour") >= (lit(6) - col("offset")) && col("hour") < (lit(12) - col("offset")), col("v_IV6"))
        .when(col("hour") >= (lit(12) - col("offset")) && col("hour") < (lit(18) - col("offset")), col("v_IV12"))
        .when(col("hour") >= (lit(18) - col("offset")) && col("hour") < (lit(24) - col("offset")), col("v_IV18"))
        .otherwise(col("v_AVT")))
    dfAVT = dfAVT
      .drop("hour", "v_IV6", "v_IV8", "v_IV12", "v_IV16", "v_IV18", "offset")
    if (debug) println(dfAVT.count())

    println("End availabilities " + new Date())

    
    println("Start mix fc + ava " + new Date())

    var dfFC_temp = dfFC
      .drop("hour")
      .join(dfAVT, (dfFC("idFG") === dfAVT("idFG") && dfFC("ts_h") === dfAVT("ts")), "left_outer")
      .drop(dfAVT("idFG"))
      .drop(dfAVT("ts"))
 
    dfFC_temp = dfFC_temp
      .na.fill(100, Array("v_AV", "v_AVT", "v_IintV2", "v_IintV3", "v_IintV4"))

    var dfFC_tempadj = dfFC_temp
      .filter(col("idFT") === ID_P50)
      .withColumn("v_ATad", col("v_AT") * col("v_AVT") / 100)
      .withColumn("v_I2ad", col("v_Iint2") * col("v_IintV2") / 100)
      .withColumn("v_I3ad", col("v_Iint3") * col("v_IintV3") / 100)
      .withColumn("v_I4ad", col("v_Iint4") * col("v_IintV4") / 100)
    dfFC_tempadj = dfFC_tempadj
      .union(dfFC_temp.filter(col("idFT") === ID_P50_ADJ)
        .withColumn("v_ATad", col("v_AT"))
        .withColumn("v_I2ad", col("v_Iint2"))
        .withColumn("v_I3ad", col("v_Iint3"))
        .withColumn("v_I4ad", col("v_Iint4")))

    dfFC_tempadj = dfFC_tempadj
      .withColumn("v_ATad", when(col("v_ATad").isNull, col("v_AT")).otherwise(col("v_ATad")))
      .withColumn("v_I2ad", when(col("v_I2ad").isNull, col("v_Iint2")).otherwise(col("v_I2ad")))
      .withColumn("v_I3ad", when(col("v_I3ad").isNull, col("v_Iint3")).otherwise(col("v_I3ad")))
      .withColumn("v_I4ad", when(col("v_I4ad").isNull, col("v_Iint4")).otherwise(col("v_I4ad")))
    if (debug) println(dfFC_tempadj.count())

    println("End mix fc + ava " + new Date())

    println("Start mix fc + capa + gl " + new Date())

    query = getQueryQUERY_FORECAST_V_CAP(dtFromFC, dtToFC)
    var dfCAP = SparkUtils.getDF(sparkSession, props.getProperty("rtdb.url"), ONTOLOGY_TS, query, schema_CAP)
    var dfMPD = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_MPD)

    dfMPD = dfMPD.select("idMeterPoint", "startValidity","endValidity").withColumnRenamed("idMeterPoint", "idMP")
    
    var dfCAPA = dfCAP
      .join(dfMPD, dfCAP("idMP") === dfMPD("idMP")
        && dfCAP("ts") >= dfMPD("startValidity")
        && dfCAP("ts") < dfMPD("endValidity"), "inner")
    dfCAPA = dfCAPA
      .groupBy("idFG", "ts").agg(sum("values"))
      .withColumnRenamed("sum(values)", "values")

    query = getQueryQUERY_GRID_LIMIT(dtFromFC, dtToFC)
    var dfGL = SparkUtils.getDF(sparkSession, props.getProperty("rtdb.url"), ONTOLOGY_AV, query, schema_GL)
    
    dfFC_tempadj = dfFC_tempadj
      .withColumn("ts", col("ts_h"))
    dfFC_tempadj = dfFC_tempadj
      .join(dfCAPA, Seq("idFG", "ts"), "left_outer")
      .withColumnRenamed("values", "v_CAP")

    dfFC_tempadj = dfFC_tempadj
      .join(dfGL.withColumnRenamed("value", "v_GL"), Seq("idFG", "ts"), "left_outer")

    dfFC_tempadj = dfFC_tempadj
      .withColumn("v_GL_orig", col("v_GL"))
    dfFC_tempadj = dfFC_tempadj
      .na.fill(Double.MaxValue, Seq("v_GL"))
    dfFC_tempadj = dfFC_tempadj
      .withColumn("v_ATad", when(col("v_ATad").gt(col("v_GL")), col("v_GL")).otherwise(col("v_ATad")))
      .withColumn("v_I2ad", when(col("v_I2ad").gt(col("v_GL")), col("v_GL")).otherwise(col("v_I2ad")))
      .withColumn("v_I3ad", when(col("v_I3ad").gt(col("v_GL")), col("v_GL")).otherwise(col("v_I3ad")))
      .withColumn("v_I4ad", when(col("v_I4ad").gt(col("v_GL")), col("v_GL")).otherwise(col("v_I4ad")))
    if (debug) println(dfFC_tempadj.count())

    println("End mix fc + capa + gl " + new Date())

    println("Start metered " + new Date())
    
        query = getQueryQUERY_METERED_BAV(dtlastMod)
    var dfBAV = SparkUtils.getDF(sparkSession, props.getProperty("rtdb.url"), ONTOLOGY_BAV, query, schema_BAV)
    var dfMPDET = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_MP, QUERY_MP_DETAILS)
    var dfMPSTA = SparkUtils.getDF(sparkSession, props.getProperty("bdb.url"), props.getProperty("bdb.user"), props.getProperty("bdb.passwd"), ONTOLOGY_MP, QUERY_METERED_MP)
    
    dfMPDET = dfMPDET.select(col("idForecastGroup").as("idFG"), concat(col("country"), lit("|")).as("country"), concat(col("client"), lit("|")).as("client"),concat(col("classMeterPoint"), lit("|")).as("class"))
    dfMPDET = dfMPDET.dropDuplicates()

    dfMPSTA = dfMPSTA.select(col("id").as("idMP"))
    dfMPSTA = dfMPSTA.dropDuplicates()       
    
    dfBAV = dfBAV
      .join(dfMPSTA,Seq("idMP"), "inner")
      .groupBy("idFG", "ts").agg(sum("v"))
      .withColumnRenamed("sum(v)", "v_Metered")
    dfFC_tempadj = dfFC_tempadj
      .join(dfBAV, Seq("idFG", "ts"), "inner")
      .join(dfMPDET, Seq("idFG"), "left_outer")

    dfFC_tempadj = dfFC_tempadj
      .na.fill(0, Seq("v_Metered"))
    dfFC_tempadj = dfFC_tempadj
      .withColumn("error_BAF", col("v_BAF") - col("v_Metered"))
      .withColumn("abserror_BAF", abs(col("v_BAF") - col("v_Metered")))
      .withColumn("errorcap_BAF", (col("v_BAF") - col("v_Metered")) / col("v_CAP"))
      .withColumn("error_DA", col("v_ATad") - col("v_Metered"))
      .withColumn("abserror_DA", abs(col("v_ATad") - col("v_Metered")))
      .withColumn("errorcap_DA", (col("v_ATad") - col("v_Metered")) / col("v_CAP"))
      .withColumn("error_I2", col("v_I2ad") - col("v_Metered"))
      .withColumn("abserror_I2", abs(col("v_I2ad") - col("v_Metered")))
      .withColumn("errorcap_I2", (col("v_I2ad") - col("v_Metered")) / col("v_CAP"))
      .withColumn("error_I3", col("v_I3ad") - col("v_Metered"))
      .withColumn("abserror_I3", abs(col("v_I3ad") - col("v_Metered")))
      .withColumn("errorcap_I3", (col("v_I3ad") - col("v_Metered")) / col("v_CAP"))
      .withColumn("error_I4", col("v_I4ad") - col("v_Metered"))
      .withColumn("abserror_I4", abs(col("v_I4ad") - col("v_Metered")))
      .withColumn("errorcap_I4", (col("v_I4ad") - col("v_Metered")) / col("v_CAP"))
    var dfOutput = dfFC_tempadj
      .select("idFG", "ts_h", "idPR", "idFT", "v_Metered", "v_CAP", "v_GL_orig", "v_AV",
        "v_BAF", "error_BAF", "abserror_BAF", "errorcap_BAF",
        "v_ATad", "v_AVT", "error_DA", "abserror_DA", "errorcap_DA",
        "v_I2ad", "v_IintV2", "error_I2", "abserror_I2", "errorcap_I2",
        "v_I3ad", "v_IintV3", "error_I3", "abserror_I3", "errorcap_I3",
        "v_I4ad", "v_IintV4", "error_I4", "abserror_I4", "errorcap_I4")
    dfOutput = dfOutput
      .withColumnRenamed("v_Cap", "capacity")
      .withColumnRenamed("v_GL_orig", "gridLimit")
      .withColumnRenamed("v_ATad", "forecast_AT")
      .withColumnRenamed("v_I2ad", "forecast_I2")
      .withColumnRenamed("v_I3ad", "forecast_I3")
      .withColumnRenamed("v_I4ad", "forecast_I4")
      .withColumnRenamed("v_AVT", "availability_AT")
      .withColumnRenamed("v_IintV2", "availability_I2")
      .withColumnRenamed("v_IintV3", "availability_I3")
      .withColumnRenamed("v_IintV4", "availability_I4")
    println(dfOutput.count())

    println("End metered + mix with fc " + new Date())
    
    println("Start write to MySQL " + new Date())
    dfOutput = dfOutput.dropDuplicates()
    println("Start dfOutput.dropDup" + new Date())
    println(dfOutput.count())
    println("End dfOutput.dropDup " + new Date())

    if (debug) dfOutput.show(10)
    //forzar que se ejecute la query

    val QUERY_DELETE_FCM_ = "delete from " + ONTOLOGY_FCM_ + " where ts_h > \"" + odtFrom + "\" and ts_h < \"" + odtTo + "\""
    restClient.deleteQuery(ONTOLOGY_FCM_, QUERY_DELETE_FCM_)

    dfOutput
      .withColumn("inserted_ts", current_timestamp())
    dfOutput
      .write.mode("append").format("jdbc")
      .option("url", props.getProperty("bdb.url"))
      .option("user", props.getProperty("bdb.user"))
      .option("password", props.getProperty("bdb.passwd"))
      .option("dbtable", ONTOLOGY_FCM_)
      .option("batchSize", 1000)
      .option("driver", "com.mysql.jdbc.Driver")
      .save()

    println("End write to MySQL " + new Date())
  }
}

        
 