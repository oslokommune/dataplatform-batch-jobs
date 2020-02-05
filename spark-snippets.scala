import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ListObjectsV2Request
import collection.mutable.Buffer
import collection.JavaConverters._

sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")

case class LogElement(bucketOwner: String, bucket: String, time: String, remoteIp: String, requester: String, requestId: String, operation: String, key: String, requestUri: String, httpStatus: String, errorCode: String, bytesSent: String, objectSize: String, totalTime: String, turnAroundTime: String, referer: String, userAgent: String, versionId: String)
val pattern = """^(\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] (\S+) (\S+) (\S+) (\S+) (\S+) (-|"[^"]*") (\d{3}) (\S+) (\S+) (\S+) (\S+) (\S+) (-|"[^"]*") (-|"[^"]*") (\S+)""".r

def parseLog(log: String): LogElement = try {
    val res = pattern.findFirstMatchIn(log)
    val List(bucketOwner, bucket, time, remoteIp, requester, requestId, operation, key, requestUri, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referer, userAgent, versionId) = res.get.subgroups
    LogElement(bucketOwner, bucket, time, remoteIp, requester, requestId, operation, key, requestUri, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referer, userAgent, versionId)
} catch { 
    case e: Exception =>
        throw new RuntimeException("Unable to parse log line: " + log, e)
}

def getLogPaths(prefix: String): Buffer[String] = {
    val s3 = AmazonS3ClientBuilder.standard.withRegion(Regions.EU_WEST_1).withCredentials(new ProfileCredentialsProvider()).build
    val req = new ListObjectsV2Request().withBucketName("ok-origo-dataplatform-logs-dev").withPrefix(prefix)
    val res = s3.listObjectsV2(req)

    res.getObjectSummaries.asScala.map(s => "s3a://" + s.getBucketName + "/" + s.getKey)
}

val paths = getLogPaths("logs/s3/ok-origo-dataplatform-dev/2020-02-03-22-")
val df = spark.read.textFile(paths:_*)
val cached = df.map(parseLog).cache
cached.count
cached.coalesce(1).write.parquet("logs/")
