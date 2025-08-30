package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"strconv"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace        = "powerdns"
	apiInfoEndpoint  = "servers/localhost"
	apiStatsEndpoint = "servers/localhost/statistics"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				c, err := net.DialTimeout(netw, addr, 5*time.Second)
				if err != nil {
					return nil, err
				}
				if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return nil, err
				}
				return c, nil
			},
		},
	}

	// package-level logger (go-kit/log)
	logger log.Logger
)

// ServerInfo is used to parse JSON data from 'server/localhost' endpoint
type ServerInfo struct {
	Kind       string `json:"type"`
	ID         string `json:"id"`
	URL        string `json:"url"`
	DaemonType string `json:"daemon_type"`
	Version    string `json:"version"`
	ConfigUrl  string `json:"config_url"`
	ZonesUrl   string `json:"zones_url"`
}

// StatsEntry is used to parse JSON data from 'server/localhost/statistics' endpoint
type StatsEntry struct {
	Name  string  `json:"name"`
	Kind  string  `json:"type"`
	Value json.RawMessage `json:"value"`
}

// Exporter collects PowerDNS stats from the given HostURL and exports them using
// the prometheus metrics package.
type Exporter struct {
	HostURL    *url.URL
	ServerType string
	ApiKey     string
	mutex      sync.RWMutex

	up                prometheus.Gauge
	totalScrapes      prometheus.Counter
	jsonParseFailures prometheus.Counter
	gaugeMetrics      map[int]prometheus.Gauge
	counterVecMetrics map[int]*prometheus.GaugeVec
	gaugeDefs         []gaugeDefinition
	counterVecDefs    []counterVecDefinition
	client            *http.Client
}

func newCounterVecMetric(serverType, metricName, docString string, labelNames []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverType,
			Name:      metricName,
			Help:      docString,
		},
		labelNames,
	)
}

func newGaugeMetric(serverType, metricName, docString string) prometheus.Gauge {
	return prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverType,
			Name:      metricName,
			Help:      docString,
		},
	)
}

// NewExporter returns an initialized Exporter.
func NewExporter(apiKey, serverType string, hostURL *url.URL) *Exporter {
	var gaugeDefs []gaugeDefinition
	var counterVecDefs []counterVecDefinition

	gaugeMetrics := make(map[int]prometheus.Gauge)
	counterVecMetrics := make(map[int]*prometheus.GaugeVec)

	switch serverType {
	case "recursor":
		gaugeDefs = recursorGaugeDefs
		counterVecDefs = recursorCounterVecDefs
	case "authoritative":
		gaugeDefs = authoritativeGaugeDefs
		counterVecDefs = authoritativeCounterVecDefs
	case "dnsdist":
		gaugeDefs = dnsdistGaugeDefs
		counterVecDefs = dnsdistCounterVecDefs
	}

	for _, def := range gaugeDefs {
		gaugeMetrics[def.id] = newGaugeMetric(serverType, def.name, def.desc)
	}

	for _, def := range counterVecDefs {
		counterVecMetrics[def.id] = newCounterVecMetric(serverType, def.name, def.desc, []string{def.label})
	}

	return &Exporter{
		HostURL:    hostURL,
		ServerType: serverType,
		ApiKey:     apiKey,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: serverType,
			Name:      "up",
			Help:      "Was the last scrape of PowerDNS successful.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverType,
			Name:      "exporter_total_scrapes",
			Help:      "Current total PowerDNS scrapes.",
		}),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: serverType,
			Name:      "exporter_json_parse_failures",
			Help:      "Number of errors while parsing PowerDNS JSON stats.",
		}),
		gaugeMetrics:      gaugeMetrics,
		counterVecMetrics: counterVecMetrics,
		gaugeDefs:         gaugeDefs,
		counterVecDefs:    counterVecDefs,
	}
}

// Describe describes all the metrics ever exported by the PowerDNS exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.counterVecMetrics {
		m.Describe(ch)
	}
	for _, m := range e.gaugeMetrics {
		ch <- m.Desc()
	}
	ch <- e.up.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.jsonParseFailures.Desc()
}

// Collect fetches the stats from configured PowerDNS API URI and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	jsonStats := make(chan []StatsEntry)

	go e.scrape(jsonStats)

	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.resetMetrics()
	statsMap := e.setMetrics(jsonStats)
	ch <- e.up
	ch <- e.totalScrapes
	ch <- e.jsonParseFailures
	e.collectMetrics(ch, statsMap)
}

func (e *Exporter) scrape(jsonStats chan<- []StatsEntry) {
	defer close(jsonStats)

	e.totalScrapes.Inc()

	var data []StatsEntry
	url := apiURL(e.HostURL, apiStatsEndpoint)
	err := getJSON(url, e.ApiKey, &data)
	if err != nil {
		e.up.Set(0)
		e.jsonParseFailures.Inc()
		level.Error(logger).Log("msg", "error scraping PowerDNS", "err", err)
		return
	}

	e.up.Set(1)

	jsonStats <- data
}

func (e *Exporter) resetMetrics() {
	for _, m := range e.counterVecMetrics {
		m.Reset()
	}
}

func (e *Exporter) collectMetrics(ch chan<- prometheus.Metric, statsMap map[string]float64) {
	for _, m := range e.counterVecMetrics {
		m.Collect(ch)
	}
	for _, m := range e.gaugeMetrics {
		ch <- m
	}

	if e.ServerType == "recursor" {
		h, err := makeRecursorRTimeHistogram(statsMap)
		if err != nil {
			level.Error(logger).Log("msg", "could not create response time histogram", "err", err)
			return
		}
		ch <- h
	}
}

func (e *Exporter) setMetrics(jsonStats <-chan []StatsEntry) (statsMap map[string]float64) {
	statsMap = make(map[string]float64)
	stats := <-jsonStats
	for _, s := range stats {
		// 1) value が number の場合
		var f float64
		if err := json.Unmarshal(s.Value, &f); err == nil {
			statsMap[s.Name] = f
			continue
		}
		// 2) value が "123" のような string の場合
		var str string
		if err := json.Unmarshal(s.Value, &str); err == nil {
			if vf, err := strconv.ParseFloat(str, 64); err == nil {
				statsMap[s.Name] = vf
			}
			continue
		}
		// 3) それ以外（配列・オブジェクトなど）は無視（Map/RingStatisticItem）
	}
	if len(statsMap) == 0 {
		return
	}

	for _, def := range e.gaugeDefs {
		if value, ok := statsMap[def.key]; ok {
			// latency gauges need to be converted from microseconds to seconds
			if strings.HasSuffix(def.key, "latency") {
				value = value / 1000000
			}
			e.gaugeMetrics[def.id].Set(value)
		} else {
			level.Error(logger).Log("msg", "expected PowerDNS stats key not found", "key", def.key)
			e.jsonParseFailures.Inc()
		}
	}

	for _, def := range e.counterVecDefs {
		for key, label := range def.labelMap {
			if value, ok := statsMap[key]; ok {
				e.counterVecMetrics[def.id].WithLabelValues(label).Set(value)
			} else {
				level.Error(logger).Log("msg", "expected PowerDNS stats key not found", "key", key)
				e.jsonParseFailures.Inc()
			}
		}
	}
	return
}

func getServerInfo(hostURL *url.URL, apiKey string) (*ServerInfo, error) {
	var info ServerInfo
	url := apiURL(hostURL, apiInfoEndpoint)
	err := getJSON(url, apiKey, &info)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func getJSON(url, apiKey string, data interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Add("X-API-Key", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf(string(content))
	}

	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(data); err != nil {
		return err
	}

	return nil
}

func apiURL(hostURL *url.URL, path string) string {
	endpointURI, _ := url.Parse(path)
	u := hostURL.ResolveReference(endpointURI)
	return u.String()
}

func main() {
	var (
		listenAddress = flag.String("listen-address", ":9120", "Address to listen on for web interface and telemetry.")
		metricsPath   = flag.String("metric-path", "/metrics", "Path under which to expose metrics.")
		apiURLFlag    = flag.String("api-url", "http://localhost:8001/", "Base-URL of PowerDNS authoritative server/recursor API.")
		apiKey        = flag.String("api-key", "", "PowerDNS API Key")
	)
	flag.Parse()

	// init logger
	logger = log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	hostURL, err := url.Parse(*apiURLFlag)
	if err != nil {
		level.Error(logger).Log("msg", "error parsing api-url", "err", err)
		os.Exit(1)
	}

	server, err := getServerInfo(hostURL, *apiKey)
	if err != nil {
		level.Error(logger).Log("msg", "could not fetch PowerDNS server info", "err", err)
		os.Exit(1)
	}

	exporter := NewExporter(*apiKey, server.DaemonType, hostURL)
	prometheus.MustRegister(exporter)

	level.Info(logger).Log("msg", "starting server", "addr", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>
             <head><title>PowerDNS Exporter</title></head>
             <body>
             <h1>PowerDNS Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})

	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		level.Error(logger).Log("msg", "http server error", "err", err)
		os.Exit(1)
	}
}

