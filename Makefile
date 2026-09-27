# reflector-reporting-relay
#
# make            build for this host
# make test       vet + tests
# make dist       release tarballs for the platforms a reflector actually runs on
# make install    install the binary, unit file and example config (needs root)

BINARY  := relay
PKG     := ./cmd/relay
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

PREFIX     ?= /usr/local
SYSCONFDIR ?= /etc
UNITDIR    ?= /etc/systemd/system
RELAY_USER ?= urfdrelay

# No cgo: mangos is pure Go, so the binary runs on a host with no libnng and no
# reflector on it.
export CGO_ENABLED=0

# Reflectors run on amd64 servers and on Raspberry Pis, so ship both arm targets.
PLATFORMS := linux/amd64 linux/arm64 linux/arm

.PHONY: all build test vet dist clean install uninstall version

all: build

build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) $(PKG)

test: vet
	go test ./...

vet:
	go vet ./...

version:
	@echo $(VERSION)

dist: test
	@rm -rf dist && mkdir -p dist
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		name=$(BINARY)-$(VERSION)-$$os-$$arch; \
		echo "  $$name"; \
		mkdir -p dist/$$name; \
		unset GOARM; [ "$$arch" = arm ] && export GOARM=7; \
		GOOS=$$os GOARCH=$$arch \
			go build -trimpath -ldflags '$(LDFLAGS)' -o dist/$$name/$(BINARY) $(PKG) || exit 1; \
		cp relay.example.yaml packaging/reflector-reporting-relay.service README.md LICENSE dist/$$name/; \
		tar -czf dist/$$name.tar.gz -C dist $$name; \
		rm -rf dist/$$name; \
	done
	@cd dist && sha256sum *.tar.gz > SHA256SUMS && echo "  SHA256SUMS"

install: build
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 0755 $(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)
	install -d $(DESTDIR)$(SYSCONFDIR)/reflector-reporting-relay
	# The config may carry a Redis password, so it is not world-readable. The
	# relay reads it as $(RELAY_USER); create that user first (see README).
	install -m 0640 -g $(RELAY_USER) relay.example.yaml \
		$(DESTDIR)$(SYSCONFDIR)/reflector-reporting-relay/relay.example.yaml
	install -d $(DESTDIR)$(UNITDIR)
	install -m 0644 packaging/reflector-reporting-relay.service $(DESTDIR)$(UNITDIR)/
	@echo
	@echo "Installed. Next:"
	@echo "  cp $(SYSCONFDIR)/reflector-reporting-relay/relay.example.yaml \\"
	@echo "     $(SYSCONFDIR)/reflector-reporting-relay/relay.yaml   # then edit it"
	@echo "  systemctl daemon-reload && systemctl enable --now reflector-reporting-relay"

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(BINARY)
	rm -f $(DESTDIR)$(UNITDIR)/reflector-reporting-relay.service
	@echo "Left $(SYSCONFDIR)/reflector-reporting-relay alone; remove it by hand if you mean to."

clean:
	rm -rf $(BINARY) dist
