package io.openexchange.storage

import com.amazonaws.services.s3.model.SelectObjectContentEvent

trait StationMeasures {
     def search(query: String, process : SelectObjectContentEvent.RecordsEvent => Unit) : Unit
}
