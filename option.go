package srclient

import (
	"net/http"
	"time"

	"golang.org/x/sync/semaphore"
)

type Option func(*SchemaRegistryClient)

func WithHttpClient(httpClient *http.Client) Option {
	return Option(func(client *SchemaRegistryClient) {
		client.httpClient = httpClient
	})
}

func WithSemaphoreLimit(n int64) Option {
	return Option(func(client *SchemaRegistryClient) {
		client.sem = semaphore.NewWeighted(n)
	})
}

func WithCredentials(username, password string) Option {
	return Option(func(client *SchemaRegistryClient) {
		if len(username) > 0 && len(password) > 0 {
			credentials := credentials{username, password}
			client.credentials = &credentials
		}
	})
}

func WithTimeout(timeout time.Duration) Option {
	return Option(func(client *SchemaRegistryClient) {
		client.httpClient.Timeout = timeout
	})
}
