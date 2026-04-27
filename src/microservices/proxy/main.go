package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithProxy      *httputil.ReverseProxy
	moviesServiceProxy *httputil.ReverseProxy
	eventsServiceProxy *httputil.ReverseProxy
	migrationPercent   int
	gradualMigration   bool
)

func main() {
	monolithURL, err := url.Parse(getEnv("MONOLITH_URL", "http://localhost:8080"))
	if err != nil {
		log.Fatal(err)
	}

	moviesURL, err := url.Parse(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	if err != nil {
		log.Fatal(err)
	}

	eventsURL, err := url.Parse(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"))
	if err != nil {
		log.Fatal(err)
	}

	monolithProxy = httputil.NewSingleHostReverseProxy(monolithURL)
	moviesServiceProxy = httputil.NewSingleHostReverseProxy(moviesURL)
	eventsServiceProxy = httputil.NewSingleHostReverseProxy(eventsURL)

	migrationPercent, _ = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	gradualMigration = getEnv("GRADUAL_MIGRATION", "false") == "true"

	port := getEnv("PORT", "8000")

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", proxyHandler)

	log.Printf("Starting proxy on :%s (gradual migration=%v, movies migration=%d%%)", port, gradualMigration, migrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/events") {
		log.Printf("→ events-service: %s %s", r.Method, r.URL.Path)
		eventsServiceProxy.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/api/movies" && routeToMoviesService() {
		log.Printf("→ movies-service: %s %s", r.Method, r.URL.Path)
		moviesServiceProxy.ServeHTTP(w, r)
		return
	}
	log.Printf("→ monolith: %s %s", r.Method, r.URL.Path)
	monolithProxy.ServeHTTP(w, r)
}

func routeToMoviesService() bool {
	if !gradualMigration {
		return false
	}
	return rand.Intn(100) < migrationPercent
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
