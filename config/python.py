from typing import List


config_requires: List[str] = []
dev_requires: List[str] = []
install_requires: List[str] = [
    "pyspark",
    "pandas",
    "numpy",
]
build_requires: List[str] = [
    "pymakehelper",
    "pydmt",
    "faker",
    "pylint",
]
test_requires: List[str] = []
requires = config_requires + install_requires + build_requires + test_requires
