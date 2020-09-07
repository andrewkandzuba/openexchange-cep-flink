package io.openexchange.aws.s3

object Application {
  def main(args: Array[String]): Unit = {
    val bucketName = args(0)
    val key = args(1)
    val query = args(2)
    val s3Client = new S3Client
    s3Client.list(bucketName)
    val json = s3Client.select(bucketName, key, query)
    println(json)
  }
}
