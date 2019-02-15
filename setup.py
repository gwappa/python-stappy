import setuptools
from stappy import VERSION_STR

setuptools.setup(
    name='stappy',
    version=VERSION_STR,
    description='a storage-access protocol for python',
    url='https://github.com/gwappa/python-stappy',
    author='Keisuke Sehara',
    author_email='keisuke.sehara@gmail.com',
    license='MIT',
    install_requires=[
        'numpy>=1.0',
        ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        ],
    packages=['stappy',],
    entry_points={
        # nothing for the time being
    }
)

