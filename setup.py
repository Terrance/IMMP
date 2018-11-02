import os.path

from setuptools import setup


README = os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.rst")

setup(name="IMMP",
      description="A modular processing platform for instant messages.",
      long_description=open(README).read(),
      packages=["immp"],
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Developers",
                   "Topic :: Communications :: Chat",
                   "Topic :: Software Development :: Libraries"])
