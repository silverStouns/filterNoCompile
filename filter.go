package ratiing_filter

import (
	"fmt"
	"github.com/pkg/errors"
	approto "proto"
	"ratings_filters/interfaces"
)

type ScopeEvent struct {
	EventsProcessor func(e []Event) error
	RatingFetcher   func() ([]*approto.RatingItem, error)
	RatingSaver     func(item []*approto.RatingItem) error
	RatingFilter    func(item *approto.RatingItem) bool
	Chunks          [][2]int
}
type ScopeDislikeReward struct {
	RatingFilter func(item *approto.RatingItem) bool
	PayerRatings interfaces.PayerRatingsDict
}

var (
	ErrNotFound = errors.New("not found")
)

func GetEvent(currentRating []*approto.RatingItem, scope ScopeEvent) error {
	filteredRating := filterRating(currentRating, scope.RatingFilter)

	previousRating, err := scope.RatingFetcher()
	if err == ErrNotFound {
		err = scope.RatingSaver(filteredRating)
		if err != nil {
			return errors.WithMessage(err, "cannot save rating")
		}
		return nil
	} else if err != nil {
		return errors.WithMessage(err, "cannot fetch rating")
	}

	events := createEvents(convertRatingToChucks(filteredRating, scope.Chunks), convertRatingToChucks(previousRating, scope.Chunks))
	err = scope.EventsProcessor(events)

	err = scope.RatingSaver(filteredRating)
	if err != nil {
		return errors.WithMessage(err, "cannot save rating")
	}

	return nil
}

func GetRewardUsers(currentRating []*approto.RatingItem, scope ScopeDislikeReward) error {
	filteredRating := filterRating(currentRating, scope.RatingFilter)

	// Получаем награды юзер с их множителями
	reward := getReward(filteredRating, scope.PayerRatings)
	for _, rew := range reward {
		fmt.Printf("reward userID:%v, FactorRuby:%v, FactorVIP:%v", rew.UserID, rew.FactorRuby, rew.FactorVIP)
		fmt.Println()
	}

	return nil
}

// filterRating фильтруем пользователей по каким то параметрам
func filterRating(rating []*approto.RatingItem, filter func(item *approto.RatingItem) bool) []*approto.RatingItem {
	var filtered []*approto.RatingItem
	for _, item := range rating {
		if filter(item) {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

const (
	Out = iota
	MoveDown
	MoveUp
	Entered
)

type Event struct {
	UserID uint32
	Event  int
}

func convertRatingToChucks(rating []*approto.RatingItem, chunks [][2]int) map[uint32]int {
	c := make(map[uint32]int)
	for idx, j := range rating {
		c[j.GetUserID()] = getChunks(idx+1, chunks)
	}
	return c
}

// сравниваем со старым
func createEvents(current, previous map[uint32]int) []Event {
	var events []Event
	// проверяем на юзеров оставшихся в рейтинге и новых
	for userID, currentChunk := range current {
		previousChunk, ok := previous[userID]
		if !ok {
			events = append(events, Event{UserID: userID, Event: Entered})
		} else if currentChunk == previousChunk {
			// pass
		} else if currentChunk > previousChunk { // рейтинг пользователя снизился
			events = append(events, Event{UserID: userID, Event: MoveDown})
		} else if previousChunk > currentChunk { // рейтинг пользователя вырос
			events = append(events, Event{UserID: userID, Event: MoveUp})
		} else {
			panic("undefined contition")
		}
	}
	for userID := range previous {
		if _, ok := current[userID]; !ok {
			events = append(events, Event{UserID: userID, Event: Out})
		}
	}
	return events
}

// getChunks получает диапазон наград юзера возвращает -1 если чанк не найден
func getChunks(rank int, chunks [][2]int) int {
	if rank == 0 {
		panic("zero rank")
	}
	for chunk, bounds := range chunks {
		if rank >= bounds[0] && rank <= bounds[1] {
			return chunk + 1 // cause zero begin indexing
		}
	}
	return -1
}

type RewardUser struct {
	UserID     uint32
	FactorRuby int64
	FactorVIP  int64
}

func getReward(rating []*approto.RatingItem, placeReward interfaces.PayerRatingsDict) []*RewardUser {
	var userReward []*RewardUser
	for _, rating := range rating {
		r := placeReward.GetReward(int(rating.GetRank()))
		reward := &RewardUser{
			UserID:     rating.GetUserID(),
			FactorRuby: r.GetFactorRuby(),
			FactorVIP:  r.GetFactorVIP(),
		}
		userReward = append(userReward, reward)
	}

	return userReward
}
