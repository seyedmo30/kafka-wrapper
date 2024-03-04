package kafkawrapper

func validateOptionalConfiguration(optionalConfiguration ...OptionalConfiguration) OptionalConfiguration {
	opt := OptionalConfiguration{
		Worker:  1,
		Retry:   1,
		Timeout: 30,
	}
	return opt
}
