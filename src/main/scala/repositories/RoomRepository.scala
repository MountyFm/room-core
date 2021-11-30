package repositories

import kz.mounty.fm.domain.room.{Room, RoomStatus}
import kz.mounty.fm.repository.BaseRepository
import org.bson.conversions.Bson
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Sorts.ascending

import scala.concurrent.Future

class RoomRepository(implicit collection: MongoCollection[Room]) extends BaseRepository {

  def roomsBySizeAndFilter(n: Int, filter: Bson): Future[Seq[Room]] =
    collection
      .find(filter)
      .sort(ascending("createdAt"))
      .limit(n)
      .collect()
      .head()
}