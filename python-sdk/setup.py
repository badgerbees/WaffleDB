#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup configuration for WaffleDB Python SDK"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="waffledb",
    version="0.2.1",
    author="WaffleDB Team",
    author_email="team@waffledb.dev",
    description="Python client for WaffleDB vector database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/waffledb/waffledb",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.28.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        "dev": ["pytest>=7.0", "black>=23.0", "mypy>=1.0"],
    },
    keywords="vector database embeddings search",
    project_urls={
        "Documentation": "https://github.com/waffledb/waffledb",
        "Source": "https://github.com/waffledb/waffledb",
        "Bug Tracker": "https://github.com/waffledb/waffledb/issues",
    },
)
