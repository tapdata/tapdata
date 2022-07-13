docker build -t `cat ./tag` .
if [[ $? -ne 0 ]]; then
    echo "docker image build failed, tag is: ""`cat ./tag`"
else
    echo "docker image build success, tag is: ""`cat ./tag`"
fi
