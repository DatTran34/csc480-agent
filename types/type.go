package types

import (
	"time"
)

//X-Auth-Token = 0d5fb989868f4421bce51517a5bbb62d
// http://api.football-data.org/v2/competitions/BL1/standings

type Competition struct {
	ID   int `json:"id"`
	Area struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"area"`
	Name        string    `json:"name"`
	Code        string    `json:"code"`
	Plan        string    `json:"plan"`
	LastUpdated time.Time `json:"lastUpdated"`
}

type Season struct {
	ID              int         `json:"id"`
	StartDate       string      `json:"startDate"`
	EndDate         string      `json:"endDate"`
	CurrentMatchday int         `json:"currentMatchday"`
	Winner          interface{} `json:"winner"`
}

type Table struct {
	Position int `json:"position" dynamodbav:"position"`
	Team     struct {
		ID       int    `json:"id" dynamodbav:"id"`
		Name     string `json:"name" dynamodbav:"name"`
		CrestURL string `json:"crestUrl" dynamodbav:"crestUrl"`
	} `json:"team" dynamodbav:"team"`
	PlayedGames    int `json:"playedGames" dynamodbav:"playedGames"`
	Won            int `json:"won" dynamodbav:"won"`
	Draw           int `json:"draw" dynamodbav:"draw"`
	Lost           int `json:"lost" dynamodbav:"lost"`
	Points         int `json:"points" dynamodbav:"points"`
	GoalsFor       int `json:"goalsFor" dynamodbav:"goalsFor"`
	GoalsAgainst   int `json:"goalsAgainst" dynamodbav:"goalsAgainst"`
	GoalDifference int `json:"goalDifference" dynamodbav:"goalDifference"`
}

type Standings struct {
	Stage string      `json:"stage"`
	Type  string      `json:"type"`
	Group interface{} `json:"group"`
	Table []Table     `json:"table" dynamodbav:"table"`
}

type Data struct {
	Competition Competition `json:"competition"`
	Season      Season      `json:"season"`
	Standings   []Standings `json:"standings"`
	Message     string      `json:"message"`
	ErrorCode   int         `json:"errorCode"`
	Error       int         `json:"error"`
}

// Status struct
type Status struct {
	Table       string `json:"table"`
	RecordCount *int64 `json:"recordCount"`
}
