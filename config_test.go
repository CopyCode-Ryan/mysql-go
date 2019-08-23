package mysqlgo

import (
	"testing"
	"errors"
)
func TestGetDSN(t *testing.T){
	t.Run("get dsn info", func(t *testing.T){
		testCases := []struct{
			in Config
			err error
		}{
			{
				in : Config{
					DSN : dsn{
						HostName	:"127.0.0.1",
						HostPort	:"3306",	
						DBName		:"dbname",
						UserName	:"",
						Password	:"",
						Charset		:"",	
						Prefix		:"",
					},
				},
				err : errors.New("getDSN : UserName is nil"),
			},
			{
				in : Config{
					DSN : dsn{
						//HostName	:"127.0.0.1",
						HostPort	:"3306",	
						DBName		:"dbname",
						UserName	:"root",
						Password	:"",
						Charset		:"",	
						Prefix		:"",
					},
				},
				err : errors.New("getDSN : HostName is nil"),
			},
		}

		for _, testCase := range testCases{
			DSN , err := testCase.in.DSN.getDSN()

			if err != nil {
				if err.Error() != testCase.err.Error() {
					t.Fatalf("get dsn fail case : %v , err :%v", testCase, err)
				}
			} else {
				t.Logf("get dsn success case : %v, dsn : %s", testCase, DSN)
			}

		}
	})
}