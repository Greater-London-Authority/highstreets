from setuptools import find_packages, setup
from dotenv import load_dotenv

setup(
    name='highstreets',
    packages=find_packages(),
    version='0.1.0',
    description='Analysis and modeling of high street profiles. ',
    author='Conor Dempsey',
    license='MIT',
    install_requires=[],
)

load_dotenv()