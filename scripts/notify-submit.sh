#!/usr/bin/env bash
# 提交一条通知的示例客户端。只用 curl + openssl，作用是把签名算法讲清楚。
#
# 用法：
#   NOTIFY_KEY_ID=local NOTIFY_SECRET=dev-secret-change-me \
#     ./scripts/notify-submit.sh <幂等键> <目标URL> [payload]
#
# 签名串（\n 分隔，顺序固定，见 spec §4.1）：
#   v1 \n METHOD \n PATH \n TIMESTAMP \n hex(sha256(body))
#
# 注意签名覆盖的是 **body 的哈希**，不只是时间戳 —— 因为 body 里装着目标 URL，
# 只签时间戳的话，中间人可以把投递目的地改成自己的服务器。

set -euo pipefail

ENDPOINT="${NOTIFY_ENDPOINT:-http://127.0.0.1:8080}"
KEY_ID="${NOTIFY_KEY_ID:?请设置 NOTIFY_KEY_ID}"
SECRET="${NOTIFY_SECRET:?请设置 NOTIFY_SECRET}"

IDEM="${1:?用法: $0 <幂等键> <目标URL> [payload]}"
TARGET="${2:?用法: $0 <幂等键> <目标URL> [payload]}"
# 默认值不能写进 ${3:-...} —— 默认值里的 `}` 会提前终止参数展开，
# 结果是 PAYLOAD 尾部多出一个 `}`，签名照样算得出来，但供应商收到的是坏 JSON。
# 这个坑由 test/acceptance/client_script_test.go 兜住。
PAYLOAD="${3:-}"
if [ -z "$PAYLOAD" ]; then
  PAYLOAD='{"event":"demo"}'
fi

API_PATH="/v1/notifications"

# target.body 是不透明字符串（spec §4.2），所以 payload 要作为 JSON 字符串嵌入。
ESCAPED=$(printf '%s' "$PAYLOAD" | sed 's/\\/\\\\/g; s/"/\\"/g')
BODY=$(printf '{"idempotency_key":"%s","target":{"url":"%s","method":"POST","headers":{"Content-Type":"application/json"},"body":"%s"}}' \
  "$IDEM" "$TARGET" "$ESCAPED")

TS=$(date +%s)
BODY_HASH=$(printf '%s' "$BODY" | openssl dgst -sha256 -hex | awk '{print $NF}')
CANON=$(printf 'v1\nPOST\n%s\n%s\n%s' "$API_PATH" "$TS" "$BODY_HASH")
SIG=$(printf '%s' "$CANON" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $NF}')

curl -sS -X POST "$ENDPOINT$API_PATH" \
  -H "Content-Type: application/json" \
  -H "X-Notify-Key-Id: $KEY_ID" \
  -H "X-Notify-Timestamp: $TS" \
  -H "X-Notify-Signature: $SIG" \
  --data-binary "$BODY" \
  -w '\n'
