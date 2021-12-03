package main

import (
	"csc482/types"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/jamespearly/loggly"
)

func getDataFromAPI() {

	var tag string = "application"

	client := loggly.New(tag)

	url := "http://api.football-data.org/v2/competitions/2021/standings"

	var api_Token = "0d5fb989868f4421bce51517a5bbb62d"

	// Create a new request using http
	req, _ := http.NewRequest("GET", url, nil)

	// add authorization header to the req
	req.Header.Add("X-Auth-Token", api_Token)

	// Send req using http Client
	http_client := &http.Client{}
	resp, err := http_client.Do(req)

	if err != nil {
		client.Send("error", "This is an error message:"+err.Error())
		log.Fatal(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		client.Send("error", "This is an error message:"+err.Error())
		panic(err)
	}
	var data types.Data
	err_ := json.Unmarshal([]byte(body), &data)

	if err_ != nil {
		client.Send("error", "This is an error message:"+err.Error())
		panic(err_)
	}

	// Valid Send (no error returned)
	output := strconv.Itoa(int(len(body)))
	err = client.EchoSend("info", "Success! Data size: "+output)
	fmt.Println("err:", err)
	createItem(data.Standings[0].Table)
}

func createItem(table []types.Table) {
	// create an aws session
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	fmt.Println("[INFO] Starting to upload to DynamoDB Table... ")
	// create a dynamodb instance
	db := dynamodb.New(sess)
	for i := 0; i < len(table); i++ {
		dynomoDB_Team, err := dynamodbattribute.MarshalMap(table[i])
		if err != nil {
			panic("Cannot marshal movie into AttributeValue map")
		}
		// create the api params
		params := &dynamodb.PutItemInput{
			TableName: aws.String("dtran3-soccer-standings"),
			Item:      dynomoDB_Team,
		}

		// put the item
		_, err_ := db.PutItem(params)
		if err_ != nil {
			fmt.Printf("ERROR: " + err.Error() + "\n")
			//sendToLoggly("ERROR: " + err.Error())
			return
		}
		fmt.Println("[INFO] Uploaded 1 item !!!  ")
	}
	fmt.Println("Success")
}

func main() {

	ticker := time.NewTicker(time.Hour * 3)

	for ; true; <-ticker.C {
		//fmt.Println("Success")
		getDataFromAPI()

	}

}
