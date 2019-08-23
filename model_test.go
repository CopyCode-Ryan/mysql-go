package mysqlgo

import (
	"fmt"
	"testing"
	"time"
	_ "github.com/go-sql-driver/mysql"
)

var config = &Config {
	Alias : "default",
	DSN : dsn{
		HostName	:"127.0.0.1",
		HostPort	:"3306",	
		DBName		:"bovine",
		UserName	:"root",
		Password	:"",
		Charset		:"utf8",	
	},
}

func TestFind(t *testing.T) {
	t.Run("TestFind", func(t *testing.T){
		testCases := []struct{
			in *Model
		}{
			{
				in : &Model {
					TableName : "b_user",
				},
			},
		}

		Connect(config)

		for _, testCase := range testCases {
			userModel := testCase.in
			user := []struct {
				Account 	string
				Password	string
			}{}
			userModel.Field("account", "password").Where("status = ?", 1).Select(&user)
			fmt.Print(userModel.Error())
			fmt.Print(userModel.LastSQL())
			fmt.Print(user)
		}
	})
}

func TestAdd(t *testing.T) {
	t.Run("db Add Data", func(t *testing.T){
		userModel := &Model {
			TableName : "b_user",
		}
		testCases := []struct{
			in []Data
		}{
			{
				in : []Data {
					{
						Field : "account",
						Value : "test3",
					},
					{
						Field : "password",
						Value : "test1",
					},
				},
			},
			{
				in : []Data {
					{
						Field : "account",
						Value : "test5",
					},
					{
						Field : "password",
						Value : "test2",
					},
					{
						Field : "last_login_time",
						Value : time.Now().Unix(),
					},
				},
			},
		}

		Connect(config)

		fmt.Print(getDBList())

		for _, testCase := range testCases {
			id, err := userModel.Add(testCase.in...)
			fmt.Print("\n")
			if err != nil {
				fmt.Print(err.Error())
			} else {
				fmt.Printf("id:%d", id)
			}
			fmt.Print("\n")
		}
	})
}

func TestVerifyField(t *testing.T) {
	t.Run("TestVerifyField", func(t *testing.T) {
		datas := [][]Data {
			{
				{
					Field : "account",
					Value : "test5",
				},
				{
					Field : "password",
					Value : "test2",
				},
				{
					Field : "last_login_time",
					Value : time.Now().Unix(),
				},
			},{
				{
					Field : "account",
					Value : "test5",
				},
				{
					Field : "test",
					Value : "test5",
				},
				{
					Field : "password",
					Value : "test2",
				},
				{
					Field : "last_login_time",
					Value : time.Now().Unix(),
				},
			},
		}
		userModel := &Model {
			TableName : "b_user",
		}
		userModel.AddAll(datas...)
	})
}