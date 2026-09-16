BIN := notifyd
PKG := ./cmd/notifyd

.PHONY: all build test test-race vet fmt run clean check

all: check build

build:
	go build -o $(BIN) $(PKG)

# 单元测试 + 验收测试（A1-A10）。验收测试会自行构建并启动真实进程。
test:
	go test ./... -timeout 600s

# 竞态检测。验收测试会用 -race 重新构建被测二进制，
# 否则只能检测测试脚手架，检测不到 notifyd 自身的并发。
test-race:
	go test ./... -race -timeout 900s

vet:
	go vet ./...

fmt:
	gofmt -w .

# 提交前的完整检查：格式、静态检查、脱敏、测试。
check:
	@test -z "$$(gofmt -l .)" || { echo "以下文件未格式化:"; gofmt -l .; exit 1; }
	go vet ./...
	./scripts/redact-check.sh
	go test ./... -timeout 600s

# 本地起一个实例（放行私网地址，便于对着本地 mock 调试）。
run: build
	@test -f keys || { printf 'local:dev-secret-change-me\n' > keys && chmod 600 keys && echo "已生成 keys（仅限本地）"; }
	NOTIFY_ALLOW_PRIVATE_HOSTS=true ./$(BIN)

clean:
	rm -f $(BIN)
	rm -rf data
