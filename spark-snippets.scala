import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ListObjectsV2Request
import collection.mutable.Buffer
import collection.JavaConverters._

sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")

case class LogElement(bucketOwner: String, bucket: String, time: String, remoteIp: String, requester: String, requestId: String, operation: String, key: String, requestUri: String, httpStatus: String, errorCode: String, bytesSent: String, objectSize: String, totalTime: String, turnAroundTime: String, referer: String, userAgent: String, versionId: String)
val logPattern = """^(\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] (\S+) (\S+) (\S+) (\S+) (\S+) (-|"[^"]*") (\d{3}) (\S+) (\S+) (\S+) (\S+) (\S+) (-|"[^"]*") (-|"[^"]*") (\S+)""".r

def parseLog(logLine: String): LogElement = try {
  val res = logPattern.findFirstMatchIn(logLine)
  val List(bucketOwner, bucket, time, remoteIp, requester, requestId, operation, key, requestUri, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referer, userAgent, versionId) = res.get.subgroups
  LogElement(bucketOwner, bucket, time, remoteIp, requester, requestId, operation, key, requestUri, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referer, userAgent, versionId)
} catch { 
  case e: Exception =>
    throw new RuntimeException("Unable to parse log line: " + logLine, e)
}

def getLogPaths(prefix: String): Buffer[String] = {
  val s3 = AmazonS3ClientBuilder.standard.withRegion(Regions.EU_WEST_1).withCredentials(new ProfileCredentialsProvider()).build
  val req = new ListObjectsV2Request().withBucketName("ok-origo-dataplatform-logs-dev").withPrefix(prefix)
  val res = s3.listObjectsV2(req)

  res.getObjectSummaries.asScala.map(s => "s3a://" + s.getBucketName + "/" + s.getKey)
}

val logPaths = getLogPaths("logs/s3/ok-origo-dataplatform-dev/2020-02-03-22-")
val rawLogs = spark.read.textFile(logPaths:_*)
val parsedLogs = rawLogs.map(parseLog).cache
parsedLogs.count
// parsedLogs.coalesce(1).write.parquet("logs/")

import org.apache.spark.sql.functions.udf

val datasetPattern = """\w+/(?:/[a-z0-9-]+/)?([a-z0-9-]+)/version""".r
def datasetId(key: String): Option[String] =
  datasetPattern.findFirstMatchIn(key).map(_.subgroups(0))

val datasetIdUDF = udf(datasetId(_))

parsedLogs.withColumn("datasetId", datasetIdUDF('key)).
  groupBy('datasetId, 'operation).
  count.
  show(false)
