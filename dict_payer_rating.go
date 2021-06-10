package helpers

import (
	"database/sql"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	pc "proto-compile"
	"mysql"
)

type dictPayerRatings struct {
	rewards []*reward
}

type reward struct {
	lBoundPlace int
	uBoundPlace int
	*pc.AdmTalkDictPayerRatingData
}

func NewDictPayerRatings(sqlPool *mysql.ConnectionsPool, tableName string) (payerRatings *dictPayerRatings, err error) {
	payerRatings = new(dictPayerRatings)

	q := "SELECT Place, Data FROM " + tableName + " WHERE Enabled = 1 ORDER BY Place"
	err = sqlPool.Select(q,
		func(rows *sql.Rows) error {
			item := &reward{
				AdmTalkDictPayerRatingData: &pc.AdmTalkDictPayerRatingData{},
			}
			var data string
			factors := &pc.AdmTalkDictPayerRatingData{}
			err := rows.Scan(&item.uBoundPlace, &data)
			if err != nil {
				return err
			}
			err = json.Unmarshal([]byte(data), &factors)
			if err != nil {
				return err
			}

			item.FactorRuby = factors.FactorRuby
			item.FactorVIP = factors.FactorVIP
			payerRatings.rewards = append(payerRatings.rewards, item)
			return nil
		})
	if err != nil {
		return nil, err
	}

	// нижняя граница
	for i, item := range payerRatings.rewards {
		if i == 0 {
			item.lBoundPlace = 1
		} else {
			item.lBoundPlace = payerRatings.rewards[i-1].uBoundPlace + 1
		}
	}

	return
}

func (d *dictPayerRatings) GetReward(place int) *pc.AdmTalkDictPayerRatingData {
	r := &pc.AdmTalkDictPayerRatingData{}
	for _, item := range d.rewards {
		if place >= item.lBoundPlace && place <= item.uBoundPlace {
			r = &pc.AdmTalkDictPayerRatingData{
				FactorRuby: item.FactorRuby,
				FactorVIP:  item.FactorVIP,
			}
		} else {
			r = &pc.AdmTalkDictPayerRatingData{
				FactorRuby: proto.Int64(0),
				FactorVIP:  proto.Int64(0),
			}
		}
	}
	return r
}
