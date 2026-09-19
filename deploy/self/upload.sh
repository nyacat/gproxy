#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 0 ]]; then
  echo "用法: $0（远端连接信息请填写 deploy/self/config.env）" >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -f "$script_dir/config.env" ]]; then
  echo "请先将 config.env.example 复制为 config.env，并填写远端配置。" >&2
  exit 1
fi
# shellcheck source-path=SCRIPTDIR
# shellcheck source=config.env.example
source "$script_dir/config.env"
: "${SSH_HOST:?请在 deploy/self/config.env 中填写 SSH_HOST}"
: "${SSH_USER:?请在 deploy/self/config.env 中填写 SSH_USER}"
if [[ "$REMOTE_TMP_DIR" != /* ]]; then
  echo "REMOTE_TMP_DIR 必须是远端绝对路径。" >&2
  exit 1
fi

ssh_options=(-T -p "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  if [[ ! -f "$SSH_KEY" ]]; then
    echo "找不到 SSH 私钥: $SSH_KEY" >&2
    exit 1
  fi
  ssh_options+=(-i "$SSH_KEY")
fi
target="$SSH_USER@$SSH_HOST"
archive="$script_dir/gproxy-self.tar.zst"
if [[ ! -s "$archive" ]]; then
  echo "找不到镜像包，请先运行 $script_dir/build.sh。" >&2
  exit 1
fi

# 一次 SSH 连接完成上传、解压和导入；归档内容通过标准输入传输。
# 将目录作为经过 POSIX shell 转义的参数传给远端 Bash。
remote_tmp_dir="'${REMOTE_TMP_DIR//\'/\'\\\'\'}'"
echo "上传到 $target 并导入镜像……"
ssh "${ssh_options[@]}" -- "$target" "$(cat <<'REMOTE'
bash -euo pipefail -c '
for command in docker zstd; do
  command -v "$command" >/dev/null || { echo "缺少远端命令: $command" >&2; exit 1; }
done
docker info >/dev/null
mkdir -p -- "$1"
archive="$(mktemp "$1/gproxy-self.XXXXXX.tar.zst")"
trap '\''rm -f -- "$archive"'\'' EXIT
trap "exit 130" INT
trap "exit 143" TERM
cat > "$archive"
zstd -dc -- "$archive" | docker image load
' --
REMOTE
) $remote_tmp_dir" < "$archive"

# 仅在远端成功导入并清理后删除本地包，失败时可直接重试。
rm -f -- "$archive"
echo "远端镜像已导入，两端压缩包已删除。已有容器需重新创建以使用新镜像。"
