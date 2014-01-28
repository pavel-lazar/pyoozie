from distutils.core import setup

setup(
    name='pyoozie',
    version='0.1.0',
    packages=['pyoozie'],
    url='https://github.com/pavel-lazar/pyoozie',
    license='Apache 2.0',
    author='Pavel Lazar',
    author_email='pavel.lazar@gmail.com',
    description='Python warpper for the Apache Oozie REST API',
    install_requires = ['requests']
)
