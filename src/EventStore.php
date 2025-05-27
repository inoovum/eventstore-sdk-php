<?php

namespace Inoovum\EventStore;

use CloudEvents\V1\CloudEvent;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;

class EventStore
{
    private string $apiUrl;
    private string $apiVersion;
    private string $authToken;
    private Client $client;

    public function __construct(string $apiUrl, string $apiVersion, string $authToken)
    {
        $this->apiUrl = $apiUrl;
        $this->apiVersion = $apiVersion;
        $this->authToken = $authToken;

        if ($this->apiUrl === '' || $this->apiVersion === '' || $this->authToken === '') {
            throw new \RuntimeException(
                'Missing required variables: apiUrl, apiVersion, authToken'
            );
        }

        $this->client = new Client([
            'headers' => [
                'User-Agent' => 'inoovum-eventstore-sdk-php',
                'Authorization' => 'Bearer ' . $this->authToken,
            ]
        ]);
    }

    /**
     * @param string $subject
     * @return CloudEvent[]
     * @throws GuzzleException
     */
    public function streamEvents(string $subject): array
    {
        $url = "{$this->apiUrl}/api/{$this->apiVersion}/stream";

        try {
            $response = $this->client->post($url, [
                'json' => ['subject' => $subject],
                'headers' => [
                    'Accept' => 'application/x-ndjson',
                    'Content-Type' => 'application/json',
                ]
            ]);

            $body = $response->getBody()->getContents();
            if (empty(trim($body))) {
                return [];
            }

            $events = [];
            $lines = explode("\n", $body);

            foreach ($lines as $line) {
                if (empty(trim($line))) {
                    continue;
                }

                try {
                    $eventData = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
                    $event = new CloudEvent(
                        $eventData['id'],
                        $eventData['source'],
                        $eventData['type'],
                        $eventData['data'],
                        'application/json',
                        null,
                        $eventData['subject'],
                        new \DateTimeImmutable($eventData['time']),
                    );
                    $events[] = $event;
                } catch (\JsonException $e) {
                    error_log("Error parsing event: " . $e->getMessage());
                    error_log("Problem with JSON: " . $line);
                }
            }

            return $events;
        } catch (GuzzleException $e) {
            error_log("Error while streaming events: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * @param array $events Array of events with subject, type, and data
     * @throws GuzzleException
     */
    public function commitEvents(array $events): void
    {
        $url = "{$this->apiUrl}/api/{$this->apiVersion}/commit";

        $requestBody = [
            'events' => array_map(function($event) {
                return [
                    'subject' => $event['subject'],
                    'type' => $event['type'],
                    'data' => $event['data']
                ];
            }, $events)
        ];

        try {
            $this->client->post($url, [
                'json' => $requestBody,
                'headers' => [
                    'Content-Type' => 'application/json',
                ]
            ]);
        } catch (GuzzleException $e) {
            error_log("Error while committing events: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * @return string
     * @throws GuzzleException
     */
    public function audit(): string
    {
        $url = "{$this->apiUrl}/api/{$this->apiVersion}/status/audit";

        try {
            $response = $this->client->get($url, [
                'headers' => [
                    'Content-Type' => 'text/plain',
                ]
            ]);

            return $response->getBody()->getContents();
        } catch (GuzzleException $e) {
            error_log("Error while running audit: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * @return string
     * @throws GuzzleException
     */
    public function ping(): string
    {
        $url = "{$this->apiUrl}/api/{$this->apiVersion}/status/ping";

        try {
            $response = $this->client->get($url, [
                'headers' => [
                    'Content-Type' => 'text/plain',
                ]
            ]);

            return $response->getBody()->getContents();
        } catch (GuzzleException $e) {
            error_log("Error while pinging: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * @param string $query The query string to execute
     * @return array Array of query results
     * @throws GuzzleException
     */
    public function q(string $query): array
    {
        $url = "{$this->apiUrl}/api/{$this->apiVersion}/q";

        try {
            $response = $this->client->post($url, [
                'json' => ['query' => $query],
                'headers' => [
                    'Accept' => 'application/x-ndjson',
                    'Content-Type' => 'application/json',
                ]
            ]);

            $body = $response->getBody()->getContents();
            if (empty(trim($body))) {
                return [];
            }

            $results = [];
            $lines = explode("\n", $body);

            foreach ($lines as $line) {
                if (empty(trim($line))) {
                    continue;
                }

                try {
                    $result = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
                    $results[] = $result;
                } catch (\JsonException $e) {
                    error_log("Error parsing result: " . $e->getMessage());
                    error_log("Problem with JSON: " . $line);
                }
            }

            return $results;
        } catch (GuzzleException $e) {
            error_log("Error while querying: " . $e->getMessage());
            throw $e;
        }
    }

}
