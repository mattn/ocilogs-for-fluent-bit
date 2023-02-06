package ocilogs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/loggingingestion"

	fluentbit "github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
)

// OutputPlugin is the CloudWatch Logs Fluent Bit output plugin
type OutputPlugin struct {
	PluginInstanceID int
	client           loggingingestion.LoggingClient

	Source   string
	Subject  string
	LogId    string
	batchMap map[string]*loggingingestion.LogEntryBatch
}

// OutputPluginConfig is the input information used by NewOutputPlugin to create a new OutputPlugin
type OutputPluginConfig struct {
	PluginInstanceID int

	Source  string
	Subject string
	LogId   string
}

type Event struct {
	TS     time.Time
	Record map[interface{}]interface{}
	Tag    string
}

// Validate checks the configuration input for an OutputPlugin instances
func (config OutputPluginConfig) Validate() error {
	errorStr := "%s is a required parameter"
	if config.Source == "" {
		return fmt.Errorf(errorStr, "source")
	}
	if config.Subject == "" {
		return fmt.Errorf(errorStr, "subject")
	}
	if config.LogId == "" {
		return fmt.Errorf(errorStr, "log_id")
	}
	return nil
}

// NewOutputPlugin creates a OutputPlugin object
func NewOutputPlugin(config OutputPluginConfig) (*OutputPlugin, error) {
	logrus.Debugf("[ocilogs %d] Initializing NewOutputPlugin", config.PluginInstanceID)

	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(common.DefaultConfigProvider())
	if err != nil {
		log.Fatalln(err.Error())
	}

	return &OutputPlugin{
		client:   client,
		Source:   config.Source,
		Subject:  config.Subject,
		LogId:    config.LogId,
		batchMap: map[string]*loggingingestion.LogEntryBatch{},
	}, nil
}

func (output *OutputPlugin) AddEvent(e *Event) int {
	m := map[string]interface{}{}
	for k, v := range e.Record {
		ks := fmt.Sprint(k)
		vv := v
		if b, ok := k.([]byte); ok {
			ks = string(b)
		}
		if b, ok := v.([]byte); ok {
			vv = string(b)
		}
		m[ks] = vv
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(m)
	if err != nil {
		return fluentbit.FLB_ERROR
	}
	var batch *loggingingestion.LogEntryBatch
	if m, ok := output.batchMap[e.Tag]; ok {
		batch = m
	} else {
		batch = &loggingingestion.LogEntryBatch{
			Source:  common.String(output.Source),
			Subject: common.String(output.Subject),
			Type:    common.String(e.Tag),
			Entries: []loggingingestion.LogEntry{},
		}
		output.batchMap[e.Tag] = batch
	}

	batch.Defaultlogentrytime = &common.SDKTime{Time: e.TS}
	batch.Entries = append(batch.Entries, loggingingestion.LogEntry{
		Data: common.String(buf.String()),
		Id:   common.String(uuid.NewString()),
		Time: &common.SDKTime{Time: e.TS},
	})
	return fluentbit.FLB_OK
}

// Flush sends the current buffer of records.
func (output *OutputPlugin) Flush() error {
	logrus.Debugf("[ocilogs %d] Flush() Called", output.PluginInstanceID)

	for _, v := range output.batchMap {
		req := loggingingestion.PutLogsRequest{
			LogId: common.String(output.LogId),
			PutLogsDetails: loggingingestion.PutLogsDetails{
				LogEntryBatches: []loggingingestion.LogEntryBatch{*v},
				Specversion:     common.String("1.0"),
			},
			TimestampOpcAgentProcessing: &common.SDKTime{Time: time.Now().UTC()},
		}

		_, err := output.client.PutLogs(context.Background(), req)
		if err != nil {
			logrus.Error(err)
		}
	}
	for k := range output.batchMap {
		delete(output.batchMap, k)
	}
	return nil
}
