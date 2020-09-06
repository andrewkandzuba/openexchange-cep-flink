package io.openexchange.aws.s3

object Application {
  def main(args: Array[String]): Unit = {
    val bucketName = args(0)
    val s3Client = new S3Client
    s3Client.list(bucketName)
  }
}
