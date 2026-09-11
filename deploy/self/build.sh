#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
  echo "用法: $0（可通过 IMAGE 环境变量设置镜像标签）" >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/../.."

for command in git docker zstd; do
  command -v "$command" >/dev/null || { echo "缺少命令: $command" >&2; exit 1; }
done
docker buildx version >/dev/null

if [[ "$(git branch --show-current)" != self ]]; then
  echo "请先切换到本地 self 分支。" >&2
  exit 1
fi

image="${IMAGE:-gproxy:self}"
archive="$script_dir/gproxy-self.tar.zst"
builder=""
archive_tmp="$(mktemp "$archive.tmp.XXXXXX")"
cleanup() {
  local status=$?
  if [[ -n "$builder" ]]; then
    docker buildx rm "$builder" >/dev/null || status=1
  fi
  rm -f -- "$archive_tmp" || status=1
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 独立 builder 的缓存随 builder 一起删除，不清理其他项目的缓存。
builder="$(docker buildx create --name "gproxy-self-$$-$RANDOM" --driver docker-container)"
echo "构建 $image（linux/amd64，通用 x86-64，release：O3 / fat LTO / 单 codegen unit）"
# 在这里覆盖 Cargo release 配置，优化参数通过 build args 传入 Docker 内的 Cargo。
# 固定通用 x86-64 指令集，兼容本地 7302、远端 7C13 和其他 x86_64 CPU。
# 直接导出 Docker tar 流，不向本地 Docker 导入应用镜像。
docker buildx build \
  --builder "$builder" \
  --platform linux/amd64 \
  --provenance=false \
  --progress plain \
  --file deploy/self/docker/Dockerfile \
  --build-arg CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
  --build-arg CARGO_PROFILE_RELEASE_LTO=fat \
  --build-arg CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  --build-arg CARGO_PROFILE_RELEASE_PANIC=abort \
  --build-arg CARGO_PROFILE_RELEASE_STRIP=symbols \
  --build-arg CARGO_PROFILE_RELEASE_DEBUG=0 \
  --build-arg CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false \
  --build-arg CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=false \
  --build-arg CARGO_PROFILE_RELEASE_INCREMENTAL=false \
  --build-arg 'RUSTFLAGS=-C target-cpu=x86-64' \
  --tag "$image" \
  --output type=docker,dest=- \
  . | zstd -T0 -f -o "$archive_tmp"

docker buildx rm "$builder" >/dev/null
builder=""
mv -f -- "$archive_tmp" "$archive"
echo "已生成 $archive；本次构建缓存已清理。"
