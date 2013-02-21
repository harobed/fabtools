import fabtools.require
from fabtools.files import is_file
from fabtools._utils import root_run


def is_pythonz_installed():
    return is_file('/usr/local/pythonz/bin/pythonz', True)


def is_python_installed(version='2.7.3'):
    return is_file('/usr/local/pythonz/pythons/CPython-%s/bin/python' % version, True)


def is_pip_installed(version='2.7.3'):
    return is_file('/usr/local/pythonz/pythons/CPython-%s/bin/pip' % version, True)


def is_virtualenv_installed(version='2.7.3'):
    return is_file('/usr/local/pythonz/pythons/CPython-%s/bin/virtualenv' % version, True)


def install(version='2.7.3'):
    fabtools.require.deb.packages([
        'libbz2-dev', 'build-essential', 'devscripts', 'libncurses5-dev',
        'libsqlite3-dev', 'libdb4.6-dev', 'libgdbm-dev',
        'libreadline5-dev', 'zlib1g-dev', 'curl', 'libssl-dev'
    ])

    if not is_pythonz_installed():
        root_run('curl -kL https://raw.github.com/saghul/pythonz/master/pythonz-install | bash')

    root_run('/usr/local/pythonz/bin/pythonz install %s' % version)


def install_pip(version='2.7.3'):
    root_run('curl http://python-distribute.org/distribute_setup.py | /usr/local/pythonz/pythons/CPython-%s/bin/python' % version)
    root_run('curl https://raw.github.com/pypa/pip/master/contrib/get-pip.py | /usr/local/pythonz/pythons/CPython-%s/bin/python' % version)


def install_virtualenv(version='2.7.3'):
    root_run('/usr/local/pythonz/pythons/CPython-%s/bin/pip install virtualenv' % version)
