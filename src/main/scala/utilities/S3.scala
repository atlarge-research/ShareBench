package utilities

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.spark.SparkConf

import java.io.ByteArrayInputStream

object S3 {
  def getClientFromSparkConf(conf: SparkConf): AmazonS3 = {
    val endpoint = conf.get("spark.hadoop.fs.s3a.endpoint")
    val accessKey = conf.get("spark.hadoop.fs.s3a.access.key")
    val secretKey = conf.get("spark.hadoop.fs.s3a.secret.key")
    val pathStyleAccess = conf.getOption("spark.hadoop.fs.s3a.path.style.access").getOrElse("false").toBoolean

    getClient(endpoint, accessKey, secretKey, pathStyleAccess = pathStyleAccess)
  }

  def getClient(endpoint: String, accessKey: String, secretKey: String, region: String = "eu-west-1", pathStyleAccess: Boolean = true): AmazonS3 = {

    val awsCreds = new BasicAWSCredentials(accessKey, secretKey)

    AmazonS3ClientBuilder
      .standard
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
      .withPathStyleAccessEnabled(true)
      .build()
  }

  def getObjectAsString(s3: AmazonS3, bucket: String, key: String): String = {
    val s3Object = s3.getObject(bucket, key)
    val s3InputStream = s3Object.getObjectContent
    val s3ObjectString = scala.io.Source.fromInputStream(s3InputStream).mkString
    s3InputStream.close()
    s3ObjectString
  }

  def putStringAsObject(s3: AmazonS3, bucket: String, key: String, content: String): Unit = {
    val contentBytes = content.getBytes("UTF-8")
    val metadata = new ObjectMetadata()
    metadata.setContentLength(contentBytes.length)
    s3.putObject(bucket, key, new ByteArrayInputStream(contentBytes), metadata)
    //    s3.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(contentBytes), metadata))
  }
}
