package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v3"

	md "github.com/gtyrin/ds/audio/metadata"
)

// MessageCmdRunner - общий интерфейс для всех сервисов, получающих запросы с помощью
// брокера сообщений.
type MessageCmdRunner interface {
	RunCmdByName(cmd string, delivery *amqp.Delivery)
	Cleanup()
}

// CmdRunner is the common microservice interface for Domestic System.
// type CmdRunner interface {
// 	RunCmdByName(cmd string, delivery *amqp.Delivery)
// }

// Request describes common request data.
type Request struct {
	Cmd    string
	Params map[string]string
}

// Version is a microservice version and build time data.
type Version struct {
	Subsystem, Name, Description, Version, Date string
}

// Service implement base functionality of microservices with using RabbitMq.
type Service struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
	msgs <-chan amqp.Delivery
	Idle bool
}

// RunModeName возвращает наименование режима запуска сервиса.
func RunModeName(idle bool) string {
	if idle {
		return "idle"
	}
	return "normal"
}

// LogLevel определяет уровень журналирования исходя из того является ли система запущенной
// в Product-режиме.
func LogLevel(isProduct bool) log.Level {
	if isProduct {
		return log.InfoLevel
	}
	return log.DebugLevel
}

// LogCmd log a queryied cmd with existing arguments.
func LogCmd(request *Request) {
	if len(request.Params) == 0 {
		log.Debug(request.Cmd + "()")
	} else {
		log.WithField("args", request.Params).Debug(request.Cmd + "()")
	}
}

// ReadConfig читает содержимое файла настройки сервиса в выходную структуру.
// Подразумевается, что файл настроек лежит в домашнем каталоге, в .config/ds подкаталоге.
func ReadConfig(optFile string, conf interface{}) {
	path, err := os.UserHomeDir()
	if err != nil {
		FailOnError(err, "Config file")
	}
	fn := filepath.Join(path, ".config/ds", optFile)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warnf("Config file %s does not exist", fn)
			data, err := yaml.Marshal(conf)
			if err != nil {
				FailOnError(err, "Config file")
			}
			if err = ioutil.WriteFile(fn, data, 0640); err != nil {
				FailOnError(err, "Config file")
			}
			log.Info("Empty option file has created")
		} else {
			FailOnError(err, "Config file")
		}
	}
	if err = yaml.Unmarshal(data, conf); err != nil {
		FailOnError(err, "Config file")
	}
}

// NewService возвращает новую копию объекта Service.
func NewService() *Service {
	return &Service{}
}

// ConnectToMessageBroker ..
func (s *Service) ConnectToMessageBroker(connstr, name string) {
	conn, err := amqp.Dial(connstr)
	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	FailOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	FailOnError(err, "Failed to register a consumer")

	s.Conn = conn
	s.Ch = ch
	s.msgs = msgs
}

// Close free RabbitMq connection resources.
func (s *Service) Close() {
	s.Ch.Close()
	s.Conn.Close()
}

// Cleanup ..
func (s *Service) Cleanup() {
	s.Close()
	fmt.Println("\nstopped")
}

// Dispatch запускает цикл обработки командных запросов.
func (s *Service) Dispatch(self MessageCmdRunner) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for delivery := range s.Msgs() {
			request, err := ParseRequest(&delivery)
			if err != nil {
				s.ErrorResult(&delivery, err, "Message dispatcher")
				continue
			}
			LogCmd(request)
			self.RunCmdByName(request.Cmd, &delivery)
		}
	}()
	log.Info("Awaiting RPC requests")
	<-c
	self.Cleanup()
	os.Exit(1)
}

// Msgs возвращает ссылку на канал передачи сообщений от клиента сервиса.
func (s *Service) Msgs() <-chan amqp.Delivery {
	return s.msgs
}

// ParseRequest check and extract request data.
func ParseRequest(d *amqp.Delivery) (*Request, error) {
	var request Request
	if err := json.Unmarshal(d.Body, &request); err != nil {
		return nil, err
	}
	return &request, nil
}

// ParseRelease parses input parameters for release request with incomplete data.
func (rq Request) ParseRelease() (*md.Release, error) {
	release := md.NewRelease()
	releaseJSON, ok := rq.Params["release"]
	if !ok {
		return nil, errors.New("Album release description is absent")
	}
	if err := json.Unmarshal([]byte(releaseJSON), release); err != nil {
		return nil, err
	}
	return release, nil
}

// RunCommonCmd execute a general command and return the result to the client.
func (s *Service) RunCommonCmd(cmd string, delivery *amqp.Delivery) {
	switch cmd {
	case "ping":
		go s.Ping(delivery)
	default:
		go s.ErrorResult(
			delivery,
			errors.New("Unknown command: "+cmd),
			"Message dispatcher")
	}
}

// ErrorResult answer to client with error info.
func (s *Service) ErrorResult(delivery *amqp.Delivery, e error, context string) {
	LogOnError(e, context)
	json := []byte(fmt.Sprintf("{\"error\": \"%s\", \"context\": \"%s\"}", e, context))
	s.Answer(delivery, json)
}

// Info add executable date of the last modification and answer service info to a client.
func (s *Service) Info(delivery *amqp.Delivery, version *Version) {
	path, err := os.Executable()
	if err != nil {
		s.ErrorResult(delivery, err, "Getting microservice executable error")
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		s.ErrorResult(delivery, err, "Getting microservice executable stat info error")
		return
	}
	version.Date = info.ModTime().Format(time.UnixDate)
	json, err := json.Marshal(version)
	if err != nil {
		s.ErrorResult(delivery, err, fmt.Sprintf("Structure conversion error for %+v", version))
		return
	}
	s.Answer(delivery, json)
}

// Ping answer about the microservice status with empty message body.
func (s *Service) Ping(delivery *amqp.Delivery) {
	s.Answer(delivery, []byte{})
}

// Answer send an answer message to client.
func (s *Service) Answer(delivery *amqp.Delivery, result []byte) {
	err := s.Ch.Publish(
		"",
		delivery.ReplyTo,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: delivery.CorrelationId,
			Body:          result,
		})
	if err != nil {
		s.ErrorResult(delivery, err, "Answer's publishing error")
		return
	}
	delivery.Ack(false)
}
