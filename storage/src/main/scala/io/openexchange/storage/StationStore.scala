package io.openexchange.storage

import io.openexchange.noaa.StationBearing

trait StationStore {
     def all : List[StationBearing]
     def search(query: String) : List[StationBearing]
}
