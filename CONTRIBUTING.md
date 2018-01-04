# Contributing

The datapackage-pipelines project accepts contributions via GitHub pull requests. This document outlines the process to help get your contribution accepted.

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

We welcome adding new processors to the standard library, the following guidelines will improve the chances of your processor being accepted:

* The processor has practical and common use-cases.
* Minimal new dependencies - preferably, no new dependencies.

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
$ pylanma
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

## Testing with other databases

By default the tests run with sqlite in-memory database which doesn't require any setup.
However, most projects will want to use a real DB, like PostgreSQL.

To run the tests with a different DB, you need to supply the connection string via environment variable.
For example, to run with local postgresql databsae:

`OVERRIDE_TEST_DB=postgresql://postgres:123456@localhost:5432/postgres py.test`
