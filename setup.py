import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = fh.read().splitlines()

setuptools.setup(
    name="pikachu",
    version="1.4.1",
    author="Maciej Gruszczyński",
    author_email="maciejgruszczynski@surferseo.com",
    description="Wrapper around pika inspired by lapin for convenient AMQP operations in Python APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/surferseo/pikachu",
    packages=["pikachu"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements,
)
