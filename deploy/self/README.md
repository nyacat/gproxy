# self 分支部署

本地需要 Bash、Git、Docker（含 Buildx）、zstd 和 SSH 客户端；远端需要
Bash、Docker、zstd，SSH 登录用户必须有运行 Docker 的权限。

首次使用时复制配置模板，再编辑 `config.env`：

```bash
cp deploy/self/config.env.example deploy/self/config.env
```

```bash
SSH_HOST="192.0.2.10"                 # 替换成远端 IP 或 SSH Host 别名
SSH_PORT=22
SSH_USER="root"
SSH_KEY="$HOME/.ssh/id_ed25519"       # 本地私钥路径；留空可使用 ssh-agent
REMOTE_TMP_DIR="/tmp"                # 远端临时目录，不存在时自动创建
```

然后在本地 `self` 分支上执行，构建包含当前工作区的修改：

```bash
./deploy/self/build.sh
./deploy/self/upload.sh
```

默认镜像名为 `gproxy:self`，产物为 `deploy/self/gproxy-self.tar.zst`，
运行时基础镜像为 `debian:13-slim`。运行约定与官方 release 镜像对齐：
工作目录 `/app`，数据目录 `/app/data`，用户和组 `65532:65532`，监听 `8787`，
入口 `/usr/local/bin/gproxy`，默认 SQLite 持久化。已有官方部署可沿用 `/app/data`
挂载和环境变量，修改镜像标签后重新创建容器；数据目录仍需允许 `65532:65532` 写入。

本目录的 `.gitignore` 忽略本地 `config.env`、镜像压缩包和构建临时文件，
`config.env.example` 作为可提交的配置模板。
Docker 相关文件统一放在 `docker/` 子目录，`docker/Dockerfile.dockerignore` 控制
Docker 构建上下文，排除依赖缓存、压缩包和 SSH 配置。构建上下文是仓库根目录，
Docker 会自动使用与 `docker/Dockerfile` 配套的忽略文件。

固定构建 `linux/amd64`（x86_64），使用 `docker/Dockerfile` 在本地 Docker 中执行
`cargo build --locked --release --target x86_64-unknown-linux-gnu`。性能参数集中在
`build.sh`，通过 Docker build args 传给 Cargo，覆盖仓库 release 配置：
`opt-level=3`、`lto="fat"`、`codegen-units=1`、`panic="abort"`，
关闭增量编译、调试断言、整数溢出检查，关闭调试信息并剥离符号。
其中 O3、fat LTO、单 codegen unit 已是仓库现有配置；这些设置以运行性能为优先，
编译会较慢且占用较多内存，实际性能提升仍需用业务负载测量。

CPU 目标固定为通用 `x86-64`，兼容本地 EPYC 7302、远端 EPYC 7C13，
也保留对其他 x86_64 CPU 的兼容性。不使用 `native`、Zen 专属目标或强制 AVX2/AVX-512。

修改镜像标签时可以设置 `IMAGE`，远端导入会保留这个标签：

```bash
IMAGE=my-gproxy:self ./deploy/self/build.sh
```

构建使用临时 Buildx builder，直接将 Docker 镜像 tar 流交给 zstd 压缩。
构建结束（包括失败）时删除本次 builder 及缓存，应用镜像不会导入本地 Docker；
本地只保留压缩包。下次构建会重新编译，不复用上次的缓存。

上传脚本通过 SSH 将包写入 `REMOTE_TMP_DIR`，解压后执行 `docker image load`，覆盖同名
标签。成功后删除两端压缩包；传输或导入失败时保留本地包供重试，远端临时包由退出
钩子清理。已有容器需另行重新创建，才会使用新镜像。
