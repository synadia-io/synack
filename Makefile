GO ?= go
CONTROLLER_GEN ?= $(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.0
IMG ?= synack:dev
DOCKER_GOOS ?= linux
DOCKER_GOARCH ?= $(shell $(GO) env GOARCH)
DOCKER_PLATFORM ?= $(DOCKER_GOOS)/$(DOCKER_GOARCH)

.PHONY: generate manifests fmt vet test build run docker-build

generate:
	$(CONTROLLER_GEN) object paths="./api/..."

manifests:
	$(CONTROLLER_GEN) crd rbac:roleName=synack-manager-role paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

fmt:
	$(GO) run mvdan.cc/gofumpt@latest -w .

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

build:
	$(GO) build ./...

run:
	$(GO) run ./main.go

docker-build:
	mkdir -p $(DOCKER_PLATFORM); \
	trap 'rm -f $(DOCKER_PLATFORM)/synack; rmdir -p $(DOCKER_PLATFORM) 2>/dev/null || true' EXIT; \
	CGO_ENABLED=0 GOOS=$(DOCKER_GOOS) GOARCH=$(DOCKER_GOARCH) $(GO) build -trimpath -ldflags="-s -w" -o $(DOCKER_PLATFORM)/synack ./main.go; \
	docker build --platform=$(DOCKER_PLATFORM) -t $(IMG) .
