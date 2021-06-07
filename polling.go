package service

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// PollingService описывает данные микросервиса, периодически обращающегося ко внешнему ресурсу.
type PollingService struct {
	*Service
	pollingFreq int64
	nextQuery   int64
	headers     map[string]string
}

// NewPollingService возвращает копию объекта PollingService.
func NewPollingService(headers map[string]string) *PollingService {
	return &PollingService{
		Service: NewService(),
		headers: headers,
	}
}

// SetPollingFrequency записывает новое значение частоты опроса внешнего ресурса.
func (s *PollingService) SetPollingFrequency(val int64) {
	s.pollingFreq = val
}

// LoadAndDecode загружает http ресурс и декодирует данные из json.
func (s *PollingService) LoadAndDecode(url string, out interface{}) error {
	data, err := s.LoadResource(url)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return err
	}
	return nil
}

// LoadResource ..
func (s *PollingService) LoadResource(url string) ([]byte, error) {
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	r.URL.RawQuery = r.URL.Query().Encode()
	for k, v := range s.headers {
		r.Header.Add(k, v)
	}
	client := http.Client{}
	s.delayQuery()
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithField("status", "fail").Error(url)
		return nil, err
	}
	s.setNextDelay()
	log.WithField("status", "ok").Debug(url)
	return data, nil
}

// TestResource вызывается ради чтения ответа сервера (например, значений заголовков).
// Вызов никак не учитывает необходимую периодичность обращений к серверу и потому
// не рекомендуется для рабочего обращения.
func (s *PollingService) TestResource(url string) (*http.Response, error) {
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	r.URL.RawQuery = r.URL.Query().Encode()
	for k, v := range s.headers {
		r.Header.Add(k, v)
	}
	client := http.Client{}
	return client.Do(r)
}

func (s *PollingService) delayQuery() {
	if s.nextQuery > 0 {
		cur := time.Now().UnixNano()
		offset := s.nextQuery - cur
		if offset > 0 {
			time.Sleep(time.Duration(offset) * time.Nanosecond)
		}
	}
}

func (s *PollingService) setNextDelay() {
	s.nextQuery = time.Now().UnixNano() + s.pollingFreq
}
