package shardkv

import "MIT6824/shardctrler"

type Command struct {
	Op   CommandType
	Data interface{}
}

type Op struct {
	Operation string // "Put" "Append" "Get"
	ClientID  int64
	MsgID     int
	Key       string
	Value     string
}

// func (command Command) String() string {
// 	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
// }

func NewOperationCommand(request *Op) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(response *ShardOperationResponse) Command {
	return Command{InsertShards, *response}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShards, *request}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)
