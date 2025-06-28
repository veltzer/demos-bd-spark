""" python dependencies for this project """

install_requires: list[str] = [
    "pyspark",
    "pandas",
    "numpy",
    "streamlit",
    "plotly",
    "setuptools",
    "wurlitzer",
    "matplotlib",
]
build_requires: list[str] = [
    "pymakehelper",
    "pydmt",
    "faker",
    "pylint",
]
requires = install_requires + build_requires
