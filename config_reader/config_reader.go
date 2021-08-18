package config_reader

import (
	"os"
    "fmt"
    "flag"
    "bufio"
	"strings"
)

// ParseArgs parses the command line arguments and
// returns the config file and topic on success, or exits on error
func ParseArgs() (*string, *string) {

	configFile := flag.String("f", "./basic_config.cfg", "Path to configuration file")
	topic := flag.String("t", "source", "Topic name")
	flag.Parse()

	return configFile, topic

}

// ReadConfig reads the file specified by configFile and
// creates a map of key-value pairs that correspond to each
// line of the file. ReadConfig returns the map on success,
// or exits on error
func ReadConfig(configFile string) map[string]string {

	m := make(map[string]string)

	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m
}