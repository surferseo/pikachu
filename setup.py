import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pikachu",
    version="0.0.1",
    author="Maciej Gruszczyński",
    author_email="maciejgruszczysnki@surferseo.com",
    description="Wrapper around pika inspired by lapin for convenient AMQP operations in Python APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/surferseo/terms-assistant",
    packages=["pikachu"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=["pika"],
)
