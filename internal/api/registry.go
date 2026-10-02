package api

import (
	"context"
	"sort"

	"github.com/PadenZach/maestro/internal/hub"
)

type healthJSON struct {
	Status bool `json:"status"`
}

type applicationJSON struct {
	Name      string `json:"name"`
	Executors int    `json:"executors"`
}

func (s *handler) jsonHealth(context.Context, *struct{}) (*jsonOutput[healthJSON], error) {
	return jsonResult(healthJSON{Status: true}), nil
}

func (s *handler) jsonExecutors(context.Context, *struct{}) (*jsonOutput[[]hub.ExecutorView], error) {
	return jsonResult(s.hub.Executors()), nil
}

func (s *handler) jsonApps(context.Context, *struct{}) (*jsonOutput[[]applicationJSON], error) {
	byApp := map[string]int{}
	for _, executor := range s.hub.Executors() {
		byApp[executor.App]++
	}
	apps := make([]applicationJSON, 0, len(byApp))
	for name, count := range byApp {
		apps = append(apps, applicationJSON{Name: name, Executors: count})
	}
	sort.Slice(apps, func(i, j int) bool { return apps[i].Name < apps[j].Name })
	return jsonResult(apps), nil
}
