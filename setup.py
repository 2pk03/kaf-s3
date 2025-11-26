from setuptools import setup, find_packages

setup(
    name="kaf-s3-connector",
    version="1.0.0",
    author="2pk03",
    description="A Python library to handle large Kafka messages using S3.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/2pk03/kaf-s3",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        "boto3>=1.20.0",
        "confluent-kafka>=1.8.2",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
