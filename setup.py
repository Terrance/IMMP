import os.path

from setuptools import setup


README = os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.rst")

setup(name="IMMP",
      description="A modular processing platform for instant messages.",
      long_description=open(README).read(),
      long_description_content_type="text/x-rst",
      license="BSD 3-Clause License",
      platforms=["Any"],
      packages=["immp", "immp.core", "immp.hook", "immp.hook.webui", "immp.plug"],
      package_data={"immp.hook.webui": ["templates/**"]},
      entry_points={"console_scripts": ["immp=immp.__main__:entrypoint"]},
      scripts=["bin/immp-migrate-hangoutsbot.py"],
      python_requires=">=3.6",
      install_requires=["aiohttp>=3.0.0"],
      extras_require={"runner": ["anyconfig>=0.9.5",
                                 "ruamel.yaml>=0.15.75"],
                      "db": ["peewee>=3.0.0"],
                      "web": ["aiohttp_jinja2>=1.0.0"],
                      "console": ["aioconsole>=0.1.14",
                                  "ptpython>=2.0.1"],
                      "sync": ["emoji>=0.5.0",
                               "jinja2>=2.6"],
                      "discord": ["discord.py>=1.0.0"],
                      "hangouts": ["hangups>=0.4.5"],
                      "telegram": ["telethon>=1.9.0"]},
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Developers",
                   "Topic :: Communications :: Chat",
                   "Topic :: Software Development :: Libraries"])
