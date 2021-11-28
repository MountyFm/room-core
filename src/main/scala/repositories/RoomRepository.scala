package repositories

import kz.mounty.fm.domain.chat.RoomMessage
import kz.mounty.fm.domain.room.{Room, RoomStatus}
import kz.mounty.fm.repository.BaseRepository
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Sorts.ascending

import scala.concurrent.Future

class RoomRepository(implicit collection: MongoCollection[Room]) extends BaseRepository {

  def roomsBySizeAndFilter(n: Int): Future[Seq[Room]] =
    collection
      .find(equal("status", RoomStatus.ACTIVE))
      .sort(ascending("createdAt"))
      .limit(n)
      .collect()
      .head()
}