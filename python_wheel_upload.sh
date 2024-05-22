python3 setup.py bdist_wheel
databricks workspace mkdirs /Shared/code
databricks workspace import --overwrite --format "AUTO" --file dist/datakickstart_dabs-0.0.1.20240319.2-py3-none-any.whl /Shared/code/datakickstart_dabs-0.0.1-py3-none-any.whl
