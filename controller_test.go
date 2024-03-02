package kafkawrapper

type ConsumerDtoMock struct {
	name string `json:"name,omitempty" validate:"required"`

	age string `json:"age" validate:"required"`
}

// func TestRun(t *testing.T) {
// 	socket := "localhost:9092"
// 	topic := "test3"
// 	groupID := "test_group5"
// 	consumer := KafkaConsumerSetup(socket, topic, groupID)
// 	Run(context.Background(), consumer, dtoType)

// }
