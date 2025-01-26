from setuptools import setup, find_packages

setup(
    name="python-simple-tasks",
    version="1.0.0",
    author="Logan Henson",
    author_email="logan@loganhenson.com",
    description="A lightweight Python task scheduler and processor using PostgreSQL.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/loganhenson/python-simple-tasks",  # GitHub repo URL
    packages=find_packages(),  # Automatically find all package directories
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "psycopg2-binary",  # PostgreSQL database driver
    ],
    entry_points={
        "console_scripts": [
            "pst=python_simple_tasks.cli:main",  # CLI command setup
        ],
    },
)
