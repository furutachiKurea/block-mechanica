# [Block Mechanica](https://github.com/furutachiKurea/block-mechanica)

```txt
    ____  __           __      __  ___          __                _           
   / __ )/ /___  _____/ /__   /  |/  /__  _____/ /_  ____ _____  (_)________ _
  / __  / / __ \/ ___/ //_/  / /|_/ / _ \/ ___/ __ \/ __ `/ __ \/ / ___/ __ `/
 / /_/ / / /_/ / /__/ ,<    / /  / /  __/ /__/ / / / /_/ / / / / / /__/ /_/ / 
/_____/_/\____/\___/_/|_|  /_/  /_/\___/\___/_/ /_/\__,_/_/ /_/_/\___/\__,_/  
```

Block Mechanica 是一个轻量化的 Kubernetes 服务，通过使用 Echo 编写的 API 服务实现 KubeBlocks 与 Rainbond 的集成

## How does it work?

[如何实现 Rainbond 与 KubeBlocks 的集成](./doc/design_document.md)

## 如何部署

[在 Rainbond 中使用 KubeBlocks](./doc/Use_KubeBlocks_in_Rainbond.md)

## 目录结构

```txt
📁 ./
├── 📁 api/
│   ├── 📁 handler/
│   ├── 📁 req/
│   └── 📁 res/
├── 📁 deploy/
│   ├── 📁 docker/
│   └── 📁 k8s/
├── 📁 doc/
│   └── 📁 assets/
├── 📁 internal/
│   ├── 📁 config/
│   ├── 📁 index/
│   ├── 📁 k8s/
│   ├── 📁 log/
│   ├── 📁 model/
│   ├── 📁 mono/
│   └── 📁 testutil/
└── 📁 service/
    ├── 📁 adapter/
    ├── 📁 backup/
    ├── 📁 builder/
    ├── 📁 cluster/
    ├── 📁 coordinator/
    ├── 📁 kbkit/
    ├── 📁 registry/
    └── 📁 resource/
```

## Make

- 构建 Docker 镜像（默认标签 latest）

  ```sh
  make image
  ```

- 构建 Docker 镜像并指定标签（如 v1.0.0）

  ```sh
  make image TAG=v1.0.0
  ```

- 构建可执行文件到 bin/block_mechanica

  ```sh
  make build
  ```

- 运行所有测试

  ```sh
  make test
  ```

- 运行指定目录下的测试（如 service 目录）

  ```sh
  make test TESTDIR=./service/...
  ```

## Contributing

[开发仓库](https://github.com/furutachiKurea/block-mechanica)

欢迎提交 PR 和 Issue，感谢您的贡献！

## License

[AGPL-3.0](./LICENSE)