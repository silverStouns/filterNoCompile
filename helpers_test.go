package helpers

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	approto "proto-compile"
	dt "packages/tests/dockertest" // внутренняя библиотека хелперов для докертеста
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"testing"
)

var redisTest redis.Pool
var mongoCollection *mongo.Collection
var sqlPool *mysql.ConnectionsPool

func TestMain(m *testing.M) {
	var mongoTest *mongo.Client
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic("create pool error: " + err.Error())
	}

	redisImage, redisTag := dt.RedisImage()
	password := "pswd"
	optsRedis := dt.RunRedisOpts{
		RunOptions: &dockertest.RunOptions{
			Hostname:   "redisX",
			Repository: redisImage,
			Tag:        redisTag,
			Cmd:        []string{"redis-server", "--port", "6300", "--requirepass", password},
		},
		Targets: []*redis.Pool{
			&redisTest,
		},
		Password: password,
	}

	resource, err := pool.Run("mongo", "3.6", nil)
	if err != nil {
		fmt.Printf("error ran mongo")
	}

	if err := pool.Retry(func() error {
		var err error
		mongoTest, err = mongo.NewClient(options.Client().ApplyURI(fmt.Sprintf("mongodb://localhost:%s",
			resource.GetPort("27017/tcp"))))
		if err != nil {
			return err
		}
		err = mongoTest.Connect(context.TODO())
		if err != nil {
			return err
		}

		return mongoTest.Ping(context.TODO(), nil)
	}); err != nil {
		fmt.Printf("init client: %v", err)
	}

	mongoCollection = mongoTest.Database("test").Collection("rating")

	mysqlImage, mysqlTag := dt.MySQLImage()
	optsSQL := dt.MySQLRunOps{
		RunOptions: &dockertest.RunOptions{
			Hostname:   "mysqlX",
			Repository: mysqlImage,
			Tag:        mysqlTag,
		},
		Targets: []**mysql.ConnectionsPool{
			&sqlPool,
		},
		Password: "",
	}

	if os.Getenv("CI") != "" {
		rootPassword := uuid.NewV4().String()

		optsRedis.Auth = docker.AuthConfiguration{
			Username:      "gitlab",
			Password:      os.Getenv("REGISTRY_PASS"),
			ServerAddress: os.Getenv("REGISTRY"),
		}
		optsSQL.Env = []string{"MYSQL_ROOT_PASSWORD=" + rootPassword}

		optsSQL.Auth = docker.AuthConfiguration{
			Username:      "gitlab",
			Password:      os.Getenv("REGISTRY_PASS"),
			ServerAddress: os.Getenv("REGISTRY"),
		}
	}

	resourceSQL, initedSql := dt.MustRunMySQL(pool, optsSQL)
	// wait for init
	<-initedSql

	_ = resourceSQL.Expire(30)

	resourceRedis, initedRedis := dt.MustRunRedis(pool, optsRedis)
	// wait for init
	<-initedRedis

	_ = resourceRedis.Expire(30)

	m.Run()

	// purge it
	err = pool.Purge(resourceRedis)
	if err != nil {
		panic("purge resource error: " + err.Error())
	}

}

func createTable(pool *mysql.ConnectionsPool) error {
	_, err := pool.Execute(
		`create schema Pimp`)
	if err != nil {
		fmt.Printf("not chema:%v", err)
		return err
	}

	_, err = pool.Execute(
		`create table DictPayerRating_Develop
(
    ID             int auto_increment
        primary key,
    Place          int                                        not null,
    LastChangeTime timestamp        default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    Enabled        tinyint unsigned default 0                 not null,
    Data           varbinary(2000)  default '' not null
)
    charset = utf8;`)
	if err != nil {
		fmt.Printf("not table:%v", err)
		return err
	}

	_, err = pool.Execute("INSERT IGNORE INTO Test.DictPayerRating_Develop SET Place=?, Enabled=1, Data=?", 1, `{"FactorRuby":10,"FactorVIP":10}`)
	if err != nil {
		return err
	}

	return err
}
func TestNewDictPayerRatings(t *testing.T) {
	err := createTable(sqlPool)
	require.NoError(t, err)
	raiting, err := NewDictPayerRatings(sqlPool, "DictPayerRating_Develop")
	require.NoError(t, err)
	require.Equal(t, int64(10), raiting.rewards[0].GetFactorRuby())
	require.Equal(t, int64(10), raiting.rewards[0].GetFactorVIP())
	reward := raiting.GetReward(1)
	require.Equal(t, int64(10), reward.GetFactorVIP())
	require.Equal(t, int64(10), reward.GetFactorRuby())
}

const redisKey = "randomKey"

func TestSaveGetRating(t *testing.T) {
	rating := []*approto.RatingItem{
		{
			UserID: proto.Uint32(1),
			Rank:   proto.Uint32(1),
			Value:  proto.Int64(100),
		}, {
			UserID: proto.Uint32(10),
			Rank:   proto.Uint32(2),
			Value:  proto.Int64(50),
		}, {
			UserID: proto.Uint32(25),
			Rank:   proto.Uint32(3),
			Value:  proto.Int64(10),
		},
	}
	err := SaveRating(redisTest, redisKey, rating)
	require.NoError(t, err)

	actualRating, err := GetPreviousRating(redisTest, redisKey)
	require.NoError(t, err)
	require.Equal(t, rating, actualRating)

	// подчистим редис
	_, err = redisTest.Do(0, "FLUSHDB")
	require.NoError(t, err)
}

// если в ключ записанно что-то не то
func TestSaveRatingErrSaved(t *testing.T) {
	_, err := redisTest.Do(0, "SET", redisKey, "random")
	require.NoError(t, err)
	_, err = GetPreviousRating(redisTest, redisKey)
	require.Error(t, err)

	// подчистим редис
	_, err = redisTest.Do(0, "FLUSHDB")
	require.NoError(t, err)
}

// если редис пустой
func TestGetEmpty(t *testing.T) {
	_, err := GetPreviousRating(redisTest, redisKey)
	require.Error(t, err)

	// подчистим редис
	_, err = redisTest.Do(0, "FLUSHDB")
	require.NoError(t, err)
}

// тест получения рейтингов из монги
func TestGetRatings(t *testing.T) {
	err := insertTestValue(mongoCollection)
	require.NoError(t, err)
	rating, err := GetRatings(mongoCollection)
	require.NoError(t, err)
	for i, j := range rating {
		require.Equal(t, j.Rank, int64(i+1))
	}

}

func insertTestValue(collection *mongo.Collection) error {
	for i := 0; i < 1000; i++ {
		_, err := collection.InsertOne(context.TODO(), bson.M{"100": i, "UserID": i})
		if err != nil {
			return err
		}
	}

	return nil
}
