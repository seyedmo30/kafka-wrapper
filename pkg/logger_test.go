package pkg

import "testing"

func TestMain1(t *testing.T) {

	l := LogSTD{
		Error:     "sample error",
		ErrorType: "error type",

		UserID:          "123",
		Username:        "user1",
		DeviceIsAndroid: true,
		ResStatus:       200,
	}

	l.Log()
}
