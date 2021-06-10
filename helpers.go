package helpers

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	approto "cporot-compile"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	r "ratings_filters/rating_filter"
	"sort"
)

//GetPreviousRating получаем предыдущий рейт пользователя из редис
func GetPreviousRating(pool redis.Pool, key string) ([]*approto.RatingItem, error) {
	var rating []*approto.RatingItem
	b, err := redis.Bytes(pool.Do(0, "GET", key))
	if err == redis.ErrNil {
		return nil, r.ErrNotFound
	} else if err != nil {
		return nil, errors.WithMessage(err, "cannot fetch rating")
	}
	err = json.Unmarshal(b, &rating)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot unmarshal rating")
	}
	return rating, nil
}

func SaveRating(pool redis.Pool, key string, rating []*approto.RatingItem) error {
	b, err := json.Marshal(rating)
	if err != nil {
		return errors.WithMessage(err, "cannot marshal rating")
	}
	_, err = pool.Do(0, "SET", key, b)
	if err != nil {
		return errors.WithMessage(err, "cannot save rating")
	}
	return nil
}

// GetDislikesByEndOfDate условная и пока теоретическая реализация, получаем слепок
func GetDislikesByEndOfDate(sqlPool *mysql.ConnectionsPool, date int64) (dislikeUser map[uint32]int64, err error) {
	err = sqlPool.SelectRow("SELECT user_id, DislikeCount FROM Talk.user_like WHERE date=?",
		func(row *sql.Row) error {
			var userID uint32
			var dislike int64
			err := row.Scan(&userID, &dislike)
			if err != nil {
				return err
			}
			dislikeUser[userID] = dislike
			return nil
		}, date)
	if err == sql.ErrNoRows {
		err = nil
	}

	return
}

type RatingsUser struct {
	UserID int64 `bson:"UserID"`
	Count  int64 `bson:"100"`
}

func GetRatings(collection *mongo.Collection) ([]*RankedUser, error) {
	var ratings []*RatingsUser

	cur, err := collection.Find(context.TODO(), bson.D{{}})
	if err != nil {
		return nil, err
	}
	defer func(cur *mongo.Cursor, ctx context.Context) {
		err := cur.Close(ctx)
		if err != nil {

		}
	}(cur, context.TODO())

	for cur.Next(context.TODO()) {

		var elem RatingsUser
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		ratings = append(ratings, &elem)
	}

	if err := cur.Err(); err != nil {
		panic(err)
	}

	return getRankedUser(ratings), nil
}

type RankedUser struct {
	Rank   int64
	UserID int64
}
type RankedUsers []*RatingsUser

func (r RankedUsers) Len() int {
	return len(r)
}

func (r RankedUsers) Less(i, j int) bool {
	if r[i].Count != r[j].Count {
		return r[i].Count > r[j].Count
	}
	return r[i].UserID < r[j].UserID
}

func (r RankedUsers) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// превращает сырой рейтинг из монги в места пользователей
func getRankedUser(rating []*RatingsUser) []*RankedUser {
	var rankUsers []*RankedUser

	sort.Sort(RankedUsers(rating))

	// присваиваем пользователям ранги
	for i, ra := range rating {
		rankedUser := &RankedUser{
			Rank:   int64(i + 1),
			UserID: ra.UserID,
		}
		rankUsers = append(rankUsers, rankedUser)
	}

	return rankUsers
}

// RatingConverter превращает рейтинги из монги в proto ratingItems(скорее всего  не понадобится) но облегчит обратную совместимость
// если потом мы захотим брать рейтинги из сервиса
func RatingConverter(rating []*RankedUser) []*approto.RatingItem {
	var pRating []*approto.RatingItem
	for _, item := range rating {
		pRating = append(pRating, &approto.RatingItem{
			Rank:   proto.Uint32(uint32(item.Rank)),
			UserID: proto.Uint32(uint32(item.UserID)),
		})
	}
	return pRating
}
