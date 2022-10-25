echo $CASE_CONFIG|base64 -d > config.yaml

python3 init/prepare_data.py
python3 init/create_datasource.py

cd cases || exit
for case_file in `ls test_*`; do
    python3 runner.py "$case_file" --core --bench 12345
done