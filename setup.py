import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="planet-api-wrapper", # Replace with your own username
    version="0.0.1",
    author="Tennison Yu",
    author_email="tennisonyu@berkeley.edu",
    description="A wrapper for getting planet satellite images",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tyu0912/planet-api-wrapper",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)