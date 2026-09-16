#!/usr/bin/env bash
# 脱敏兜底检查：提交前扫描仓库内的文本文件，查找疑似未脱敏内容。
# 零依赖：只用 POSIX 工具 + grep -E。
#
# 两类规则：
#   1. 通用模式（写死在本脚本里）—— 邮箱、私网 IP、本机家目录路径、常见密钥形态。
#   2. 项目私有词表 —— 真实公司名/内网域名等本身就是敏感词，不能写进公开仓库。
#      放在 scripts/redact-patterns.local（已 gitignore），每行一个 ERE。
#
# 退出码：0 = 干净；1 = 有命中，需人工确认。

set -u
cd "$(dirname "$0")/.."

FAIL=0
LOCAL_PATTERNS="scripts/redact-patterns.local"

# 扫描范围：跟踪中的文本文件 + 未跟踪但非忽略的文件；排除 .git 与二进制。
files() {
  { git ls-files 2>/dev/null; git ls-files --others --exclude-standard 2>/dev/null; } \
    | sort -u \
    | grep -v -E '^(scripts/redact-patterns\.local|\.git/)' \
    | while IFS= read -r f; do
        [ -f "$f" ] || continue
        case "$(LC_ALL=C file -b --mime-type "$f" 2>/dev/null)" in
          text/*|application/json|application/javascript|inode/x-empty) printf '%s\n' "$f" ;;
        esac
      done
}

scan() {
  # scan <标签> <命中模式ERE> [<白名单模式ERE，命中则忽略该行>]
  local label="$1" pattern="$2" allow="${3:-}" hits
  hits=$(files | xargs -I{} grep -nE -- "$pattern" {} /dev/null 2>/dev/null || true)
  if [ -n "$hits" ] && [ -n "$allow" ]; then
    hits=$(printf '%s\n' "$hits" | grep -vE -- "$allow" || true)
  fi
  if [ -n "$hits" ]; then
    printf '\n[命中] %s\n' "$label"
    printf '%s\n' "$hits" | sed 's/^/    /'
    FAIL=1
  fi
}

echo "== 脱敏检查 =="

# --- 通用规则 ---
# 邮箱。RFC 2606 保留域名（example.com/net/org、*.test/.invalid）是合法占位符，放行。
scan "邮箱地址" '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}' \
                '@(example\.(com|net|org)|[A-Za-z0-9.-]+\.(test|invalid|localhost))\b'
# 私网 IP（RFC1918）
scan "私网 IP 地址"      '(^|[^0-9.])(10\.[0-9]{1,3}|192\.168|172\.(1[6-9]|2[0-9]|3[01]))\.[0-9]{1,3}\.[0-9]{1,3}'
# 本机绝对路径
scan "本机家目录绝对路径" '/(Users|home)/[A-Za-z0-9._-]+/'
# 常见密钥形态
scan "疑似密钥/令牌"      '(gh[pousr]_[A-Za-z0-9]{16,}|sk-[A-Za-z0-9]{16,}|AKIA[0-9A-Z]{12,}|xox[abprs]-[A-Za-z0-9-]{10,}|eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,})'
# 硬编码口令
scan "疑似硬编码口令"     '(password|passwd|secret|api[_-]?key|token)[[:space:]]*[:=][[:space:]]*["'"'"'][^"'"'"']{6,}'
# 飞书/企业 IM 用户标识
scan "IM open_id"        '\bou_[a-f0-9]{24,}\b'

# --- 项目私有词表 ---
if [ -f "$LOCAL_PATTERNS" ]; then
  n=0
  while IFS= read -r p; do
    case "$p" in ''|\#*) continue ;; esac
    n=$((n+1))
    scan "私有词表规则 #$n" "$p"
  done < "$LOCAL_PATTERNS"
  echo "-- 已加载私有词表：$n 条"
else
  echo "-- 未找到 $LOCAL_PATTERNS（仅通用规则生效）"
  echo "   建议创建该文件，每行一个正则：真实公司名 / 内网域名 / 内部服务名 / 真实姓名"
fi

echo
if [ "$FAIL" -eq 0 ]; then
  echo "✅ 未发现疑似敏感信息"
else
  echo "❌ 发现疑似敏感信息，请逐条确认后再提交"
  echo "   （若确为误报，改写措辞或收窄规则，不要直接跳过检查）"
fi
exit "$FAIL"
