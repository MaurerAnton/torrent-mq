"""
torrent-mq Python SDK — Package Setup Script

Installation
------------
    pip install .
    pip install -e .       # editable / development mode
    pip install .[snappy]  # with Snappy compression support
    pip install .[lz4]     # with LZ4 compression support
    pip install .[zstd]    # with Zstandard compression support
    pip install .[all]     # all optional features

Requirements
------------
    Python 3.8 or later.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="torrent-mq",
    version="0.1.0",
    author="Nous Research",
    author_email="engineering@nousresearch.com",
    description="Python client SDK for torrent-mq distributed messaging",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NousResearch/torrent-mq",
    packages=find_packages(include=["torrent_mq", "torrent_mq.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.8",
    install_requires=[
        "crc32c>=2.4",
    ],
    extras_require={
        "snappy": ["python-snappy>=0.6"],
        "lz4": ["lz4>=4.0"],
        "zstd": ["zstandard>=0.22"],
        "all": [
            "crc32c>=2.4",
            "python-snappy>=0.6",
            "lz4>=4.0",
            "zstandard>=0.22",
        ],
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=23.0",
            "isort>=5.12",
            "mypy>=1.0",
            "flake8>=6.0",
        ],
    },
    keywords=[
        "torrent-mq",
        "messaging",
        "streaming",
        "distributed",
        "kafka",
        "pubsub",
        "producer",
        "consumer",
    ],
    project_urls={
        "Documentation": "https://hermes-agent.nousresearch.com/docs",
        "Source": "https://github.com/NousResearch/torrent-mq",
        "Tracker": "https://github.com/NousResearch/torrent-mq/issues",
    },
)
