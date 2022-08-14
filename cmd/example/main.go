package main

import (
	logrusloki "github.com/schoentoon/logrus-loki"
	"github.com/sirupsen/logrus"
)

func main() {
	hook, err := logrusloki.NewLokiDefaults("http://127.0.0.1:3100/loki/api/v1/push")
	if err != nil {
		logrus.Fatal(err)
	}
	defer hook.Close()

	log := logrus.New()
	log.AddHook(hook)

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"number": 0,
	}).Trace("Went to the beach")

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"number": 8,
	}).Debug("Started observing beach")

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")

	log.WithFields(logrus.Fields{
		"omg":    true,
		"number": 122,
	}).Warn("The group's number increased tremendously!")

	log.WithFields(logrus.Fields{
		"temperature": -4,
	}).Debug("Temperature changes")
}
