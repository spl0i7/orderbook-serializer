package main

type SerializationManager interface {
	Serialize(SerializableData) error
	Start()
	Stop()
}

type SerializableData struct {
	Key  string
	Data []byte
}
