package interfaces

import  pc "proto-compile"

type PayerRatingsDict interface {
	GetReward(place int) *pc.AdmTalkDictPayerRatingData
}
