package kafkawrapper

// validateOptionalConfiguration validates and sets optional configurations for the Kafka wrapper.
// If no optional configurations are provided, it returns default configurations.
// Otherwise, it overrides default configurations with the provided values, if they are valid.
func validateOptionalConfiguration(optionalConfiguration ...OptionalConfiguration) OptionalConfiguration {
	// Default configurations
	opt := OptionalConfiguration{
		Worker:  1,
		Retry:   1,
		Timeout: 30,
	}

	// If no optional configurations provided, return default configurations
	if len(optionalConfiguration) == 0 {
		return opt
	}

	// Override default configurations with provided values
	config := optionalConfiguration[0]

	// Validate and set Retry value
	if config.Retry > 1 {
		opt.Retry = config.Retry
	}

	// Validate and set Timeout value
	if config.Timeout > 1 {
		opt.Timeout = config.Timeout
	}

	// Validate and set Worker value
	if config.Worker > 1 {
		opt.Worker = config.Worker
	}

	return opt
}
