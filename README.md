# etcd-ydb

Etcd degradates after achieving critical amount of data. Etcd developers recommend not to store more than 8GB in cluster. The idea of the project is to overcome this obstacle by creating a system, implementing etcd API, that is able to work with large amount of data and providing the same garantees as etcd - fault-tolerance and strong constitency.

## Prerequisites

### devcontainers/cli

```bash
npm install -g @devcontainers/cli
```

## Requirements

- docker 24.0.7
- docker compose 2.21.0
- devcontainers/cli (optional)

## Build

### Dev Container Cli

Also, you can build a project with [devcontainers](https://containers.dev/) in an easy and convenient way.  
Your IDE (e.g. Clion) or code editor (e.g. VS Code) can run and attach to devcontainer.  

You can use devcontainers/cli to set up environment and build the project manually via bash:

```bash
devcontainer up --workspace-folder .
```
