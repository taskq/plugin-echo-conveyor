package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/rs/zerolog/log"
)

var PluginName string = "EchoConveyor"
var PluginDescription string = "Echo Conveyor plugin for TaskQ Subscriber"
var BuildVersion string = "0.0.0"

type ConfigurationStruct struct {
	UpstreamPublisherURL     *url.URL `json:"publisher_url"`
	UpstreamPublisherChannel string   `json:"publisher_channel"`
}

type PublisherMessageStruct struct {
	Channel string `json:"channel"`
	Payload string `json:"payload"`
}

func PublishMessage(payload []byte, channel string, serverURL *url.URL) (result bool, err error) {

	serverURL.Path = path.Join(serverURL.Path, "put")

	PublisherMessage := PublisherMessageStruct{
		Channel: channel,
		Payload: string(payload),
	}

	message, err := json.Marshal(PublisherMessage)
	if err != nil {
		fmt.Println(err)
		return false, err
	}

	log.Info().
		Str("plugin", PluginName).
		Bytes("payload", payload).
		Str("channel", channel).
		Str("message", string(message)).
		Int("quotes_num", strings.Count(string(message), `"`)).
		Msgf("Preparing to publish a message")

	req, err := http.NewRequest("POST", serverURL.String(), bytes.NewBuffer(message))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return false, err
	}

	defer response.Body.Close()

	// fmt.Printf("serverURL Path: '%v'\n", serverURL.Path)
	// fmt.Printf("response Status: '%v'\n", response.Status)
	// fmt.Println("response Headers:", resp.Header)
	// body, _ := ioutil.ReadAll(resp.Body)
	// fmt.Println("response Body:", string(body))

	return true, nil
}

func ExecCommand(payload []byte, configurationRaw interface{}) (result []byte, err error) {

	configuration := ConfigurationStruct{}

	if configurationRaw, ok := configurationRaw.(map[string]interface{}); ok {

		for key, value := range configurationRaw {

			if value, ok := value.(string); ok {

				switch {

				case key == "publisher_url":

					configuration.UpstreamPublisherURL, err = url.Parse(value)
					if err != nil {
						return nil, fmt.Errorf("Couldn't parse UpstreamPublisherURL: %v", err)
					}

				case key == "publisher_channel":
					configuration.UpstreamPublisherChannel = value

				default:
					return nil, fmt.Errorf("Unhandled configuration key: '%+v'", key)
				}

			} else {
				return nil, fmt.Errorf("Configuration key '%+v': unhandled value type (expected string)", key)

			}
		}

	} else {
		return nil, fmt.Errorf("Unhandled configuration parameter structure (expected map[string]interface{})")
	}

	log.Info().
		Str("plugin", PluginName).
		Str("UpstreamPublisherChannel", configuration.UpstreamPublisherChannel).
		Msgf("Configuration read")

	// _, err = PublishMessage(payload, configuration.UpstreamPublisherChannel, configuration.UpstreamPublisherURL)
	publish_result, err := PublishMessage(payload, configuration.UpstreamPublisherChannel, configuration.UpstreamPublisherURL)
	if err != nil {
		return nil, fmt.Errorf("Couldn't publish message to upstream: %v", err)
	}

	log.Info().
		Str("plugin", PluginName).
		Bool("publish_result", publish_result).
		Msgf("Published")

	// fmt.Printf("Publishing result: %v\n", publish_result)

	return payload, nil
}

func main() {

	serverURL := url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8080",
		Path:   "/put",
	}

	configuration := ConfigurationStruct{
		UpstreamPublisherURL:     &serverURL,
		UpstreamPublisherChannel: "junk",
	}

	_, _ = ExecCommand([]byte("beep-boop"), configuration)

}
