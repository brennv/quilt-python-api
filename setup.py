from setuptools import setup


def readme():
    with open('README.rst', 'r') as f:
        return f.read()


setup(
    name='quiltdata',
    packages=['quiltdata'],
    version='0.1.0',
    description='Quilt Python API https://quiltdata.com',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        # for more options see https://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    author='Kevin Moore',
    author_email='???@???.com',  # same as you regestered
    license='???',  # you will probably have to add a LICENSE.txt to the repo
    url='https://github.com/quiltdata/API',
    download_url='https://github.com/quiltdata/API/tarball/0.1.0',
    keywords='quiltdata api social shareable data platform',
    install_requires=[
        'requests',
        'numpy',
        'pandas',
        'psycopg2',
        'sqlalchemy',
        # probably more
    ],
    include_package_data=True,
    zip_safe=False)