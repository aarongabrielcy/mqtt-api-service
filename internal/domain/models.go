package domain

import "time"

type RawMessage struct {
ID        string
DeviceID  string
Brand     string
Topic     string
Payload   interface{}
Timestamp time.Time
}

type DeadLetterMessage struct {
ID        string
Topic     string
Payload   []byte
Error     string
Timestamp time.Time
}

type Device struct {
ID       string
Brand    string
Model    string
Type     string
Status   string
LastSeen time.Time
}
