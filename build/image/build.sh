push=$1
if [[ $push == "-p" ]]; then
    push=1
else
    push=0
fi

cross_platform_build=0
docker_main_version=`docker --version|awk '{print $3}'|awk -F "." '{print $1}'`
docker_minor_version=`docker --version|awk '{print $3}'|awk -F "." '{print $2}'`
if [[ $docker_main_version -gt 19 ]]; then
    cross_platform_build=1
else
    if [[ $docker_main_version -eq 19 ]]; then
        if [[ $docker_minor_version -gt 3 ]]; then
            cross_platform_build=1
        fi
    fi
fi
if [[ $push -eq 1 && $cross_platform_build -eq 1 ]]; then
    echo "building cross platform env image and push it to image repo"
    docker buildx build -t `cat ./tag` --platform=linux/arm64,linux/amd64 . --push
else
    docker build . -t `cat ./tag`
fi
echo "docker dev image build success, tag is: ""`cat ./tag`"
