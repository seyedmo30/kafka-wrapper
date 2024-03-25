package pkg

import "testing"

func TestMain1(t *testing.T) {

	l := StdLog{
		Error:     "sample error",
		ErrorType: "ErrorType",

		UserID:          "123",
		Username:        "user1",
		DeviceIsAndroid: true,
		ResStatus:       200,
	}

	l.Log("test error")
}
