import setuptools


with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name="transport_broker",
    python_requires=">=3.7",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=["tests"]),
    include_package_data=True,
    version="0.1.0",
    description=(
        "The library provides a way to simplify working with message brokers"
    ),
    author="Nikita Artimovich",
    author_email="artimovichn@gmail.com",
    url="https://githgub.com/nartim88/transport",
    keywords=["kafka", "broker"],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: "
        "Libraries :: Python Modules",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Operating System :: MacOS",
    ],
    install_requires=[
        "kafka-python",
        "aiokafka",
    ]
)
