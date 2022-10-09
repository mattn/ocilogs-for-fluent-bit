package main

import (
	"C"
	"fmt"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/mattn/ocilogs-for-fluent-bit/ocilogs"

	"github.com/sirupsen/logrus"
)

var (
	pluginInstances []*ocilogs.OutputPlugin
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)

	config := getConfiguration(ctx, pluginID)
	err := config.Validate()
	if err != nil {
		return err
	}

	instance, err := ocilogs.NewOutputPlugin(config)
	if err != nil {
		return err
	}

	output.FLBPluginSetContext(ctx, pluginID)
	pluginInstances = append(pluginInstances, instance)

	logrus.SetLevel(logrus.DebugLevel)
	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *ocilogs.OutputPlugin {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "ocilogs", "OCI Logs Fluent Bit Plugin!")
}

func getConfiguration(ctx unsafe.Pointer, pluginID int) ocilogs.OutputPluginConfig {
	config := ocilogs.OutputPluginConfig{}
	config.PluginInstanceID = pluginID

	config.Source = output.FLBPluginConfigKey(ctx, "source")
	logrus.Infof("[ocilogs %d] plugin parameter source = '%s'", pluginID, config.Source)

	config.Subject = output.FLBPluginConfigKey(ctx, "subject")
	logrus.Infof("[ocilogs %d] plugin parameter subject = '%s'", pluginID, config.Subject)

	config.LogId = output.FLBPluginConfigKey(ctx, "log_id")
	logrus.Infof("[ocilogs %d] plugin parameter log_id = '%s'", pluginID, config.LogId)

	return config
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	logrus.Debug("A new higher performance OCI Logs plugin has been released; ")

	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Error(err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	plugin := getPluginInstance(ctx)

	fluentTag := C.GoString(tag)
	logrus.Debugf("[ocilogs %d] Found logs with tag: %s", plugin.PluginInstanceID, fluentTag)

	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now()
		}

		retCode := plugin.AddEvent(&ocilogs.Event{Tag: fluentTag, Record: record, TS: timestamp})
		if retCode != output.FLB_OK {
			return retCode
		}
		count++
	}
	err := plugin.Flush()
	if err != nil {
		fmt.Println(err)
		// TODO: Better error handling
		return output.FLB_RETRY
	}

	logrus.Debugf("[ocilogs %d] Processed %d events", plugin.PluginInstanceID, count)

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
