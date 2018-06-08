
### INSTALL

dalmatian requires `matplotlib`, which requires when using pyenv and virtualenv that python is enabled as a framework at install.

See 'Working with Matplotlib on OSX' in the Matplotlib FAQ for more information.

In practice this setting `PYTHON_CONFIGURE_OPTS` env var prior to install, as shown below:

```
PYTHON_CONFIGURE_OPTS="--enable-framework" pyenv install x.x.x
```

If using `pyenv`, first create and activate virtual environment

```
# create virtualenv
pyenv virtualenv 3.6.1 dalmatian-3.6.1

pyenv activate dalmatian-3.6.1
```

use `setup.py`

```
python setup.py install
```

**NOTE:** For Python 3.6 due to a FISS dependency, `pylint` must be installed *first*

```
pip install pylint
python setup.py install

```

Alternative: manually add packages

```
pip install matplotlib
pip install pandas
pip install pytz
pip install firecloud
pip install iso8601
```
