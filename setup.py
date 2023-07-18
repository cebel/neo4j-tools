#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=7.0',
    'tqdm==4.64.1',
    'pandas==1.5.1',
    'neo4j==5.2.0',
    'networkx==3.0',
    'pygraphviz==1.10',
    'yfiles_jupyter_graphs==1.4.4']

test_requirements = ['pytest>=3', ]

setup(
    author="Christian.Ebeling",
    author_email='Christian.Ebeling@SCAI.Fraunhofer.de',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Neo4J tools for most of the standard procedures.",
    entry_points={
        'console_scripts': [
            'neo4j_tools=neo4j_tools.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='neo4j_tools',
    name='neo4j_tools',
    packages=find_packages(include=['neo4j_tools', 'neo4j_tools.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/cebel/neo4j-tools',
    version='0.1.0',
    zip_safe=False,
)
