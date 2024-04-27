package dcp

import (
	"fmt"
)

type Config struct {
	Host          string            `json:"host" yaml:"host"`
	Username      string            `json:"username" yaml:"username"`
	Password      string            `json:"password" yaml:"password"`
	Database      string            `json:"database" yaml:"database"`
	ChannelBuffer uint              `json:"channelBuffer" yaml:"channelBuffer"`
	Publication   PublicationConfig `json:"publication" yaml:"publication"`
	Slot          SlotConfig        `json:"slot" yaml:"slot"`
}

type PublicationConfig struct {
	Name         string   `json:"name" yaml:"name"`
	Create       bool     `json:"create" yaml:"create"`
	DropIfExists bool     `json:"dropIfExists" yaml:"dropIfExists"`
	ScopeTables  []string `json:"scopeTables" yaml:"scopeTables"`
	All          bool     `json:"all" yaml:"all"`
}

type SlotConfig struct {
	Name   string `json:"name" yaml:"name"`
	Create bool   `json:"create" yaml:"create"`
}

func (c Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?replication=database", c.Username, c.Password, c.Host, c.Database)
}
