FROM	python:3.8

WORKDIR	/usr/src/app

COPY	requirements.txt /usr/src/app/
RUN	pip3 install --no-cache-dir -r requirements.txt

ENV	PYTHONPATH /usr/src/app
CMD	[ "python3", "-m", "immp", "config.yaml" ]

ARG	uid=1000
ARG	gid=1000

RUN	groupadd -g $gid immp
RUN	useradd -u $uid -g immp immp

COPY	. /usr/src/app/
RUN	pip3 install --no-cache-dir .

VOLUME	/data
WORKDIR	/data

USER	immp
