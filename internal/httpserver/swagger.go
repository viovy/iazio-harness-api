package httpserver

import (
	_ "embed"
	"net/http"
)

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Iazio Harness API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css" />
  <style>
    html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
    *, *:before, *:after { box-sizing: inherit; }
    body { margin: 0; background: #fafafa; font-family: sans-serif; }
    .topbar { display: none; }
  </style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
<script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-standalone-preset.js"></script>
<script>
window.onload = () => {
  window.ui = SwaggerUIBundle({
    url: '/swagger/openapi.json',
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    layout: "StandaloneLayout"
  });
};
</script>
</body>
</html>
`

const openAPISpecJSON = `{
  "openapi": "3.0.3",
  "info": {
    "title": "Iazio Harness Control Plane API",
    "version": "1.0.0",
    "description": "Control plane REST API for prompt scheduling, worker host orchestration, git repo checkouts, and live execution logging."
  },
  "servers": [
    {
      "url": "/",
      "description": "Default server"
    }
  ],
  "paths": {
    "/": {
      "get": {
        "summary": "Root service status",
        "responses": {
          "200": { "description": "Service information" }
        }
      }
    },
    "/version": {
      "get": {
        "summary": "Version information",
        "responses": {
          "200": { "description": "Build and version info" }
        }
      }
    },
    "/healthz": {
      "get": {
        "summary": "Liveness check",
        "responses": {
          "200": { "description": "Service alive" }
        }
      }
    },
    "/v1/ideas": {
      "get": {
        "summary": "List ideas",
        "responses": {
          "200": { "description": "Array of ideas" }
        }
      }
    },
    "/v1/ideas/import-gemini": {
      "post": {
        "summary": "Import idea from Gemini share URL",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "url": { "type": "string" }
                },
                "required": ["url"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Imported idea" },
          "400": { "description": "Invalid payload" }
        }
      }
    },
    "/v1/ideas/{id}/triage": {
      "post": {
        "summary": "Triage idea status",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "status": { "type": "string" }
                },
                "required": ["status"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Updated idea" },
          "404": { "description": "Idea not found" }
        }
      }
    },
    "/v1/prompts": {
      "get": {
        "summary": "List prompts",
        "responses": {
          "200": { "description": "Array of prompts" }
        }
      },
      "post": {
        "summary": "Create a new prompt",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "title": { "type": "string" },
                  "body": { "type": "string" },
                  "engine": { "type": "string" },
                  "story_id": { "type": "string" },
                  "source_idea_id": { "type": "string" }
                },
                "required": ["title", "body"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Created prompt" },
          "400": { "description": "Invalid prompt payload" }
        }
      }
    },
    "/v1/prompts/{id}": {
      "put": {
        "summary": "Update prompt",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "title": { "type": "string" },
                  "body": { "type": "string" },
                  "engine": { "type": "string" }
                }
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Updated prompt" },
          "404": { "description": "Prompt not found" }
        }
      },
      "delete": {
        "summary": "Delete prompt",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "204": { "description": "Prompt deleted" },
          "404": { "description": "Prompt not found" }
        }
      }
    },
    "/v1/prompts/{id}/execute": {
      "post": {
        "summary": "Schedule prompt execution on a target repo",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "host_id": { "type": "string" },
                  "path": { "type": "string" },
                  "iterations": { "type": "integer" },
                  "env": { "type": "object", "additionalProperties": { "type": "string" } }
                },
                "required": ["host_id", "path"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Created schedule" },
          "404": { "description": "Prompt or repo not found" }
        }
      }
    },
    "/v1/schedules/{id}/cancel": {
      "post": {
        "summary": "Cancel schedule",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Schedule cancelled" },
          "404": { "description": "Schedule not found" }
        }
      }
    },
    "/v1/schedules/{id}/priority": {
      "post": {
        "summary": "Adjust schedule priority",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "delta": { "type": "integer" }
                },
                "required": ["delta"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Updated schedule" },
          "404": { "description": "Schedule not found" }
        }
      }
    },
    "/v1/hosts": {
      "get": {
        "summary": "List worker hosts",
        "responses": {
          "200": { "description": "Array of hosts" }
        }
      }
    },
    "/v1/hosts/register": {
      "post": {
        "summary": "Register a new host",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "id": { "type": "string" },
                  "kind": { "type": "string" }
                },
                "required": ["id"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Registered host" }
        }
      }
    },
    "/v1/hosts/{id}": {
      "get": {
        "summary": "Host detail",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Host detail" },
          "404": { "description": "Host not found" }
        }
      }
    },
    "/v1/hosts/{id}/heartbeat": {
      "post": {
        "summary": "Host heartbeat",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Heartbeat acknowledged" },
          "404": { "description": "Host not found" }
        }
      }
    },
    "/v1/hosts/{id}/poll": {
      "post": {
        "summary": "Poll host for executable schedule leases",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Leased job or empty" },
          "404": { "description": "Host not found" }
        }
      }
    },
    "/v1/hosts/{host}/repos": {
      "get": {
        "summary": "Repo detail for a host",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "path", "in": "query", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Repo detail including queue, lock, schedules, running_job, and history" },
          "404": { "description": "Repo not found" }
        }
      },
      "delete": {
        "summary": "Unregister repo",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "path", "in": "query", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "204": { "description": "Repo unregistered" },
          "404": { "description": "Repo not found" }
        }
      }
    },
    "/v1/repos": {
      "post": {
        "summary": "Register or upsert repo",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "host_id": { "type": "string" },
                  "path": { "type": "string" },
                  "docs_hub_path": { "type": "string" },
                  "clone_url": { "type": "string" },
                  "default_branch": { "type": "string" }
                },
                "required": ["host_id", "path"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Upserted repo" }
        }
      }
    },
    "/v1/repos/{host}/resume": {
      "post": {
        "summary": "Resume paused repo queue",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "path": { "type": "string" }
                },
                "required": ["path"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Repo queue resumed" },
          "404": { "description": "Repo not found" }
        }
      }
    },
    "/v1/repos/{host}/force-pause": {
      "post": {
        "summary": "Force pause repo queue",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "path": { "type": "string" },
                  "reason": { "type": "string" }
                },
                "required": ["path"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Repo queue paused" },
          "404": { "description": "Repo not found" }
        }
      }
    },
    "/v1/repos/{host}/preflight": {
      "post": {
        "summary": "Report preflight status for a leased job",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Preflight accepted" }
        }
      }
    },
    "/v1/repos/{host}/finish": {
      "post": {
        "summary": "Report execution finish for a leased job",
        "parameters": [
          { "name": "host", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Execution finish recorded" }
        }
      }
    },
    "/v1/repos/discard": {
      "post": {
        "summary": "Request discard and reset of a repo working copy",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "host_id": { "type": "string" },
                  "path": { "type": "string" }
                },
                "required": ["host_id", "path"]
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Discard status" }
        }
      }
    },
    "/v1/jobs/{id}": {
      "get": {
        "summary": "Get job detail",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Job detail" },
          "404": { "description": "Job not found" }
        }
      }
    },
    "/v1/jobs/{id}/logs": {
      "get": {
        "summary": "Get job logs",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "page", "in": "query", "schema": { "type": "integer" } },
          { "name": "cursor", "in": "query", "schema": { "type": "string" } },
          { "name": "from_seq", "in": "query", "schema": { "type": "integer" } },
          { "name": "to_seq", "in": "query", "schema": { "type": "integer" } }
        ],
        "responses": {
          "200": { "description": "Job logs page" }
        }
      }
    },
    "/v1/jobs/{id}/stream": {
      "get": {
        "summary": "Stream live job logs (SSE)",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Text event stream" }
        }
      }
    },
    "/v1/jobs/{id}/chunks": {
      "post": {
        "summary": "Append log chunk from runner process",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Chunk accepted" }
        }
      }
    },
    "/v1/jobs/{id}/decline": {
      "post": {
        "summary": "Decline leased job",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Job declined" }
        }
      }
    },
    "/v1/jobs/{id}/exit": {
      "post": {
        "summary": "Post child process exit status",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Exit recorded" }
        }
      }
    },
    "/v1/jobs/{id}/cancel": {
      "post": {
        "summary": "Request job cancellation",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Job cancelled" }
        }
      }
    }
  }
}
`

func (s *Server) swaggerUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerHTML))
}

func (s *Server) openAPISpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openAPISpecJSON))
}
