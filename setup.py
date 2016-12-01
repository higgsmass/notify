import os
from setuptools import setup

def read(descfile):
    return open( os.path.join ( os.path.dirname(__file__), descfile)).read()

setup(
    name='notify',

    version='1.0',

    author='Venkat Kaushik',

    author_email='higgsmass@gmail.com',

    maintainer='Venkat Kaushik',

    maintainer_email='higgsmass@gmail.com',

    url='https://github.com/higgsmass/notify',

    description= ('Cookie cutter python module with Vagrant test environment') ,

    long_description = read('README.md'),

    platforms="platform-independent",

    license='MIT',

    packages=['notify'],

    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Text Processing :: Linguistic',
    ],

    install_requires = [
        'warnings',
    ],

    setup_requires = [
        'pbr',
    ],

    test_suite='nose.collector',
    tests_require=['nose'],

    entry_points = {
        'console_scripts': ['notify-cmd=notify.command_line:main'],
    },

    scripts=[
        'bin/bl-update',
        'bin/bulk-load'
    ],

    pbr=True,
    zip_safe=False,
    include_package_data=True,

)
