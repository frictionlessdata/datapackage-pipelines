# Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

## Getting Started

Recommended way to get started is to create and activate a project virtual environment.

You should ensure you are using a supported Python version, you can check the .travis.yml to see which versions we use for CI.

* [Pythonz](https://github.com/saghul/pythonz#installation) can be used to install a specific Python version.
* [Virtualenvwrapper](http://virtualenvwrapper.readthedocs.io/en/latest/install.html#basic-installation) can help setting up and managing virtualenvs

To install package and development dependencies into active environment:

```
$ make install
```

## Lint & Test

Before pushing code you should ensure lint and tests pass otherwise build will fail and your Pull request won't be merged :(

You can use the following snippet to ensure everything works:

```
make install && make lint && make test
```


## Linting

To lint the project codebase:

```
$ make lint
```

Under the hood `pylama` configured in `pylama.ini` is used. On this stage it's already
installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://www.pylint.org/.

For example to check only errors:

```
$ pylama
```

## Testing

To run tests with coverage:

```
$ make test
```
Under the hood `tox` powered by `py.test` and `coverage` configured in `tox.ini` is used.
It's already installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://testrun.org/tox/latest/.

For example to check subset of tests against Python 3 environment with increased verbosity.
All positional arguments and options after `--` will be passed to `py.test`:

```
tox -e py35 -- -v tests/<path>
```
