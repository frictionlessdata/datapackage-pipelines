# tests for the pipelines standard library

## fixtures

Each file in the fixtures sub-directory corresponds to paramaters of test to run.

The parameters are laid out in the file, separated by `\n--\n`

This is the order of parameters:

* `processor` - name of the processor to run
* `params` - parameters
* `dp_in` - input datapackage
* `data_in` - input data
* `dp_out` - expected output datapackage
* `data_out` - expected output data

## setting up the test environment and running a specific test

* `pip install pytest`
* `py.test -svk name-of-the-fixture`
