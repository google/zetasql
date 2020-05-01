run:
	docker run -it --rm -v `pwd`:/work/zetasql/ bazel
build:
	DOCKER_BUILDKIT=1 docker build -t bazel -f ./docker/Dockerfile .
build-formatter: build
	mv ./zetasql-kotlin/build/*_jar.jar ~/.Trash/
	docker run -it --rm -v `pwd`:/work/zetasql/ \
		-v /var/run/docker.sock:/var/run/docker.sock \
		bazel
.PHONY: run build
