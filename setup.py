from setuptools import setup, find_packages

setup(
    name = "lcp_efh_automation",
    version = "0.0.1",
    author = "Abdullah Reza",
    author_email = "rashid.reza@lightcastlebd.com",
    description = "Python script for data automation",
    long_description = """
                        Python script for automating data pipeline. Data is stored in MySQL database and
                        the script reads and analyze the data before saving the processed data in a
                        table. The post processed table data is displayed in Google Data Studio.
                    """,
    # packages=find_packages(),
    py_modules = ["gds_automation"],
    install_requires = [
        "pandas",
        "SQLAlchemy",
        "PyMySQL",
        "cryptography",
        "python-dotenv"
    ],
    keywords = ["data orchestration", "data pipeline", "data automation"],
    python_requires = '>=3.8, <3.10',
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Data Automation",
        "License :: OSI Approved :: Apache License 2.0",
        "Programming Language :: Python :: 3.8"
    ]
)