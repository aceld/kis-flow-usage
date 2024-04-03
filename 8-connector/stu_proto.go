package main

type StuScore1_2 struct {
	StuId  int `json:"stu_id"`
	Score1 int `json:"score_1"`
	Score2 int `json:"score_2"`
}

type StuScoreAvg struct {
	StuId    int     `json:"stu_id"`
	AvgScore float64 `json:"avg_score"`
}

type StuScore3 struct {
	StuId      int     `json:"stu_id"`
	AvgScore12 float64 `json:"avg_score_1_2"` // score_1, score_2 avg
	Score3     int     `json:"score_3"`
}
