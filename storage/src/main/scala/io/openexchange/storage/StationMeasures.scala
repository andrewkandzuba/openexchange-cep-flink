package io.openexchange.storage

trait StationMeasures {
  def search(query: String): List[String]
}
