echo $CASE_CONFIG|base64 -d > config.yaml

python3 init/prepare_data.py
python3 init/create_datasource.py

cd cases || exit
python3 runner.py --case test_dev_sync.py --bench 100