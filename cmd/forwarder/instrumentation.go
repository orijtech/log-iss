package main

import (
	"log"
	"net/http"

	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"

	"github.com/orijtech/otils"
)

var (
	tagKeyRequestID, _  = tag.NewKey("request_id")
	tagKeyRemoteAddr, _ = tag.NewKey("remote_addr")
)

var mux = http.NewServeMux()

func init() {
	// Sampling rate: 1 in 10000
	trace.SetDefaultSampler(trace.ProbabilitySampler(0.0001))

	se, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID: otils.EnvOrAlternates("STACKDRIVER_PROJECT_ID", "census-demos"),
	})
	if err != nil {
		log.Fatalf("Stackdriver NewExporter err: %v", err)
	}
	trace.RegisterExporter(se)
	view.RegisterExporter(se)

	if err := view.Subscribe(ochttp.DefaultViews...); err != nil {
		log.Fatalf("DefaultViews subscription err: %v", err)
	}

	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: otils.EnvOrAlternates("LOG_ISS_PROMETHEUS_NAMESPACE", "log_iss_local"),
	})
	if err != nil {
		log.Fatalf("Prometheus NewExporter err: %v", err)
	}
	view.RegisterExporter(pe)
	mux.Handle("/metrics", pe)
}
