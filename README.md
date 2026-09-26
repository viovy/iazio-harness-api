# iazio-harness-api

HTTP control plane for ideas, prompts, hosts, and repo queues.

The transcript store is a separate service. This process does not call a model.

```text
go test ./...
go build -o iazio-harness-api ./cmd/iazio-harness-api
```

`GET /version` returns the linked informational version.

`POST /v1/ideas/import-gemini` accepts `{ "share_url": "https://gemini.google.com/share/<id>" }` only. One page render runs at a time. A second request receives HTTP 429.

Public modules in this family: `iazio`, `iazio-api`, `iazio-web`, `iazio-harvester`, `iazio-mcp`, `iazio-agent`, `iazio-harness`.
