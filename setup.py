from setuptools import find_packages, setup

setup(
    name='mgb',
    packages=find_packages(include=['mypythonlib']),
    version='0.1.0',
    description='Mining Groups Behavior',
    author='Abdelouahab Chibah',
    license='',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)
