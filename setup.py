from glob import glob
import os.path

from setuptools import find_namespace_packages, setup


README = os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.rst")


BASE = "immp"
PACKAGES = [BASE]
DATA = {}
SCRIPTS = glob("bin/*")

for name in find_namespace_packages(BASE):
    pkg = "{}.{}".format(BASE, name)
    parent, sub = pkg.rsplit(".", 1)
    if parent and sub == "templates":
        DATA[parent] = ["{}/**".format(sub)]
    else:
        PACKAGES.append(pkg)


AIOHTTP = "aiohttp>=3.0.0"


setup(name="IMMP",
      description="A modular processing platform for instant messages.",
      long_description=open(README).read(),
      long_description_content_type="text/x-rst",
      license="BSD 3-Clause License",
      platforms=["Any"],
      packages=PACKAGES,
      package_data=DATA,
      scripts=SCRIPTS,
      entry_points={"console_scripts": ["immp=immp.__main__:entrypoint"]},
      python_requires=">=3.7",
      extras_require={"runner": ["anyconfig>=0.11.1",
                                 "ruamel.yaml>=0.15.75"],
                      "uv": ["uvloop>=0.12.0"],
                      "db": ["tortoise-orm>=0.15.0"],
                      "web": [AIOHTTP,
                              "aiohttp_jinja2>=1.0.0"],
                      "webui": ["docutils>=0.16"],
                      "console": ["aioconsole>=0.1.14"],
                      "sync": ["emoji>=1.2.1",
                               "jinja2>=2.6"],
                      "discord": [AIOHTTP,
                                  "discord.py>=2.0.0",
                                  "emoji>=1.6.2"],
                      "hangouts": [AIOHTTP,
                                   "hangups>=0.4.11"],
                      "slack": [AIOHTTP,
                                "emoji>=1.6.2"],
                      "telegram": [AIOHTTP,
                                   "telethon>=1.9.0"]},
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Developers",
                   "Topic :: Communications :: Chat",
                   "Topic :: Software Development :: Libraries"])
