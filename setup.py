from setuptools import setup, find_packages

setup(
    name="python-simple-tasks",
    version="1.0.0",
    author="Logan Henson",
    author_email="logan@loganhenson.com",
    description="A lightweight Python task scheduler and processor using PostgreSQL.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/loganhenson/python_simple_tasks",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
    install_requires=[
        'psycopg2>=2.8',  # PostgreSQL database driver
        "python-dotenv>=0.21"  # Dependency for loading .env files
    ],
    entry_points={
        "console_scripts": [
            "pst=python_simple_tasks.cli:main",  # CLI command setup
        ],
    },
)
