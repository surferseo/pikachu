![Pikachu Bunny](pikachu_bunny.png)
Wrapper around [pika](https://pika.readthedocs.io/en/stable/) inspired by [lapin](https://hexdocs.pm/lapin/main.html) for convenient AMQP operations in Python APIs.

### Development

This library is beeing developed in context of Python APIs. To run those APIs with pikachu library imported from local repository, you have to:

- clone _pikachu_ repository
- add repository path to `PYTHONPATH` e.g.
  ```bash
  export PYTHONPATH=/Users/maciek/Repositories/pikachu:$PYTHONPATH
  ```
  `PYTHONPATH` has to be set in terminal from which you run your python app.
