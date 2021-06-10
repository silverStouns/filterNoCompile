package ratiing_filter

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	approto "proto"
	"redis"
	"ratings_filters/interfaces"
	"reflect"
	"testing"
)

const (
	normal   = "key"
	notFound = "errKey"
	errKey   = "err"
)

type TestdictPayerRatings struct {
}

func (d *TestdictPayerRatings) GetReward(place int) interfaces.Reward {
	if place == 1 {
		return interfaces.Reward{
			FactorRuby: 10,
			FactorVip:  10,
		}
	} else if place == 2 {
		return interfaces.Reward{
			FactorRuby: 7,
			FactorVip:  7,
		}
	} else if place == 3 {
		return interfaces.Reward{
			FactorRuby: 5,
			FactorVip:  5,
		}
	}
	return interfaces.Reward{}
}

//GetPreviousRating получаем предыдущий рейт пользователя из редис
func getPreviousRating(_ redis.Pool, key string) ([]*approto.RatingItem, error) {
	if key == normal {
		return []*approto.RatingItem{
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
		}, nil
	} else if key == notFound {
		return nil, ErrNotFound
	}
	return nil, fmt.Errorf("not saved")

}

func saveRating(_ redis.Pool, key string, _ []*approto.RatingItem) error {
	if key == normal || key == notFound {
		return nil
	}
	return fmt.Errorf("not saved ratings")
}
func TestRun(t *testing.T) {
	chunks := [][2]int{
		{1, 2},
		{2, 10},
		{11, 30},
		{31, 50},
	}

	currentRating := []*approto.RatingItem{
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

	expectedFilteredRating := []*approto.RatingItem{
		{
			UserID: proto.Uint32(1),
			Rank:   proto.Uint32(1),
			Value:  proto.Int64(100),
		}, {
			UserID: proto.Uint32(25),
			Rank:   proto.Uint32(3),
			Value:  proto.Int64(10),
		},
	}

	expectedEvents := []Event{
		{UserID: 10, Event: Out},
		{UserID: 25, Event: MoveUp},
	}

	err := GetEvent(currentRating, ScopeEvent{
		EventsProcessor: func(events []Event) error {
			for _, e := range events {
				require.Contains(t, expectedEvents, e)
			}
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return currentRating, nil
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			require.True(t, reflect.DeepEqual(expectedFilteredRating, r))
			return nil
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return item.GetUserID() == 10
		},
		Chunks: chunks,
	})

	require.NoError(t, err)

	err = GetEvent(currentRating, ScopeEvent{
		EventsProcessor: func(events []Event) error {
			require.Len(t, events, 0)
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return currentRating, nil
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			require.True(t, reflect.DeepEqual(currentRating, r))
			return nil
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		Chunks: chunks,
	})

	require.NoError(t, err)

	err = GetEvent(nil, ScopeEvent{
		EventsProcessor: func(e []Event) error {
			fmt.Printf("process event: %+v\n", e)
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return getPreviousRating(nil, notFound)
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			return saveRating(nil, notFound, r)
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		Chunks: chunks,
	})
	require.NoError(t, err)

	err = GetEvent(nil, ScopeEvent{
		EventsProcessor: func(e []Event) error {
			fmt.Printf("process event: %+v\n", e)
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return getPreviousRating(nil, errKey)
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			return saveRating(nil, errKey, r)
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		Chunks: chunks,
	})
	require.Error(t, err)

	err = GetEvent(nil, ScopeEvent{
		EventsProcessor: func(e []Event) error {
			fmt.Printf("process event: %+v\n", e)
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return getPreviousRating(nil, notFound)
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			return saveRating(nil, errKey, r)
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		Chunks: chunks,
	})
	require.Error(t, err)

	err = GetEvent(nil, ScopeEvent{
		EventsProcessor: func(e []Event) error {
			fmt.Printf("process event: %+v\n", e)
			return nil
		},
		RatingFetcher: func() ([]*approto.RatingItem, error) {
			return getPreviousRating(nil, normal)
		},
		RatingSaver: func(r []*approto.RatingItem) error {
			return saveRating(nil, errKey, r)
		},
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		Chunks: chunks,
	})

	require.Error(t, err)

	f := TestdictPayerRatings{}
	err = GetRewardUsers(currentRating, ScopeDislikeReward{
		RatingFilter: func(item *approto.RatingItem) bool {
			return false
		},
		PayerRatings: &f,
	})
	require.NoError(t, err)
}

func TestFilterRating(t *testing.T) {

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
	expected := []*approto.RatingItem{
		{
			UserID: proto.Uint32(1),
			Rank:   proto.Uint32(1),
			Value:  proto.Int64(100),
		}, {
			UserID: proto.Uint32(25),
			Rank:   proto.Uint32(3),
			Value:  proto.Int64(10),
		},
	}

	filtered := filterRating(rating, func(item *approto.RatingItem) bool {
		return item.GetUserID() == 10
	})

	require.True(t, reflect.DeepEqual(expected, filtered))
}

func TestGetChunks(t *testing.T) {
	definedChunks := [][2]int{
		{1, 5},
		{6, 10},
	}
	var (
		ranks          = []int{1, 5, 6, 100}
		expectedChunks = []int{1, 1, 2, -1}
	)
	for idx, rank := range ranks {
		require.Equal(t, getChunks(rank, definedChunks), expectedChunks[idx], idx)
	}

	defer func() {
		if x := recover(); x == nil {
			t.Fatalf("panic expected")
		}
	}()

	getChunks(0, definedChunks)
}

func TestConvertToChunks(t *testing.T) {
	definedChunks := [][2]int{
		{1, 2},
		{3, 10},
	}
	rating := []*approto.RatingItem{
		{
			UserID: proto.Uint32(1),
			Rank:   proto.Uint32(1),
			Value:  proto.Int64(100),
		}, {
			UserID: proto.Uint32(25),
			Rank:   proto.Uint32(3),
			Value:  proto.Int64(10),
		}, {
			UserID: proto.Uint32(50),
			Rank:   proto.Uint32(100),
			Value:  proto.Int64(1),
		},
	}

	expectedChunks := map[uint32]int{1: 1, 25: 1, 50: 2}
	chunks := convertRatingToChucks(rating, definedChunks)
	require.True(t, reflect.DeepEqual(expectedChunks, chunks))
}

func TestCreateEvents(t *testing.T) {

	currentChunks := map[uint32]int{10: 1, 20: 2, 30: 3, 40: 5, 50: 6}
	previousChunks := map[uint32]int{20: 1, 30: 4, 40: 5, 50: 6, 60: 7}
	expectedEvents := []Event{
		{UserID: 10, Event: Entered},
		{UserID: 20, Event: MoveDown},
		{UserID: 30, Event: MoveUp},
		{UserID: 60, Event: Out},
	}

	events := createEvents(currentChunks, previousChunks)
	for _, e := range events {
		require.Contains(t, expectedEvents, e)
	}
}
